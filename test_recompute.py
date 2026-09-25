"""校准谱系与重算平台的端到端场景测试。"""

import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from service import make_handler
from recompute import (
    DomainError,
    IV_ACTIVE,
    IV_PENDING,
    PLAN_AWAITING_APPROVAL,
    PLAN_COMPLETED,
    Platform,
    Store,
    UNIT_DONE,
    UNIT_SKIPPED,
)


def build_platform(recompute_fn=None):
    platform = Platform(recompute_fn=recompute_fn)
    platform.register_participants(
        [
            {"participant_id": "p1", "consent_scopes": ["recompute"]},
            {"participant_id": "p2", "consent_scopes": ["recompute"]},
            {"participant_id": "p3", "consent_scopes": ["enrollment"]},  # 同意范围不足
            {"participant_id": "p4", "consent_scopes": ["recompute"]},  # 稍后撤回
        ]
    )
    platform.register_device({"device_id": "mri-a", "center_id": "center-north", "model": "X"})
    platform.register_device({"device_id": "mri-b", "center_id": "center-south"})
    return platform


def seed_scans_and_metrics(platform):
    platform.register_scans(
        [
            {"scan_id": "s1", "visit_id": "v1", "participant_id": "p1", "device_id": "mri-a",
             "sequence": "T1", "acquired_at": "2025-03-10T09:00:00Z"},
            {"scan_id": "s2", "visit_id": "v2", "participant_id": "p2", "device_id": "mri-a",
             "sequence": "T1", "acquired_at": "2025-03-15T09:00:00Z"},
            {"scan_id": "s3", "visit_id": "v3", "participant_id": "p3", "device_id": "mri-a",
             "sequence": "T1", "acquired_at": "2025-03-16T09:00:00Z"},
            {"scan_id": "s4", "visit_id": "v4", "participant_id": "p4", "device_id": "mri-a",
             "sequence": "T2", "acquired_at": "2025-03-17T09:00:00Z"},
            # 另一台设备：不应受 mri-a 校准影响
            {"scan_id": "s5", "visit_id": "v5", "participant_id": "p1", "device_id": "mri-b",
             "sequence": "T1", "acquired_at": "2025-03-11T09:00:00Z"},
            # 区间外（更换完成后）采集
            {"scan_id": "s6", "visit_id": "v6", "participant_id": "p1", "device_id": "mri-a",
             "sequence": "T1", "acquired_at": "2025-05-02T09:00:00Z"},
        ]
    )
    platform.register_metrics(
        [
            {"metric_id": "m1", "visit_id": "v1", "sequence": "T1", "name": "volume",
             "source_scan_ids": ["s1"], "value": 100.0},
            {"metric_id": "m2", "visit_id": "v2", "sequence": "T1", "name": "volume",
             "source_scan_ids": ["s2"], "value": 110.0},
            {"metric_id": "m3", "visit_id": "v3", "sequence": "T1", "name": "volume",
             "source_scan_ids": ["s3"], "value": 120.0},
            {"metric_id": "m4", "visit_id": "v4", "sequence": "T2", "name": "fa",
             "source_scan_ids": ["s4"], "value": 0.5},
            {"metric_id": "m5", "visit_id": "v5", "sequence": "T1", "name": "volume",
             "source_scan_ids": ["s5"], "value": 130.0},
            {"metric_id": "m6", "visit_id": "v6", "sequence": "T1", "name": "volume",
             "source_scan_ids": ["s6"], "value": 140.0},
        ]
    )
    platform.register_analyses(
        [
            {"analysis_id": "a-frozen", "frozen": True, "from_stage": "premenopause",
             "to_stage": "perimenopause", "input_metric_ids": ["m1", "m2"]},
            {"analysis_id": "a-live", "frozen": False, "from_stage": "premenopause",
             "to_stage": "postmenopause", "input_metric_ids": ["m1", "m5"]},
        ]
    )


COIL_WINDOW = {
    "notice_id": "n1", "device_id": "mri-a", "valid_from": "2025-03-01T00:00:00Z",
    "calibration_version": "coil-window-corrected-v1", "params": {"correction_factor": 1.05},
}
CORRECTION = {
    "notice_id": "n2", "device_id": "mri-a", "valid_from": "2025-05-01T00:00:00Z",
    "calibration_version": "coil-replaced-v2", "params": {"correction_factor": 1.0},
}


class LineageAndNoticeTest(unittest.TestCase):
    def setUp(self):
        self.platform = build_platform()

    def test_lineage_chain_closes_predecessor(self):
        self.platform.register_calibration_notice(COIL_WINDOW)
        self.platform.register_calibration_notice(CORRECTION)
        lineage = self.platform.device_lineage("mri-a")
        self.assertEqual([iv["calibration_version"] for iv in lineage["intervals"]],
                         ["coil-window-corrected-v1", "coil-replaced-v2"])
        first, second = lineage["intervals"]
        self.assertEqual(first["valid_to"], "2025-05-01T00:00:00Z")
        self.assertIsNone(second["valid_to"])
        self.assertEqual(first["superseded_by"], second["interval_id"])
        self.assertEqual(second["predecessor_id"], first["interval_id"])

    def test_duplicate_notice_is_idempotent(self):
        first = self.platform.register_calibration_notice(COIL_WINDOW)
        self.assertFalse(first["duplicate"])
        again = self.platform.register_calibration_notice(COIL_WINDOW)
        self.assertTrue(again["duplicate"])
        lineage = self.platform.device_lineage("mri-a")
        self.assertEqual(len(lineage["intervals"]), 1)

    def test_contradictory_params_pause_interval_until_adjudication(self):
        self.platform.register_calibration_notice(COIL_WINDOW)
        conflicting = dict(COIL_WINDOW, notice_id="n-conflict",
                           calibration_version="coil-window-v1b",
                           params={"correction_factor": 0.9})
        result = self.platform.register_calibration_notice(conflicting)
        self.assertIn("contradiction", result)
        ct = result["contradiction"]
        self.assertEqual(len(self.platform.open_contradictions("mri-a")), 1)
        lineage = self.platform.device_lineage("mri-a")
        self.assertEqual(lineage["intervals"][0]["status"], IV_PENDING)

        with self.assertRaises(DomainError) as error:
            self.platform.create_plan({"trigger_notice_id": "n1"})
        self.assertEqual(error.exception.code, "interval_pending_adjudication")

        # 第三个参数版本并入同一矛盾记录，不重复开单。
        third = dict(COIL_WINDOW, notice_id="n-conflict2",
                     calibration_version="coil-window-v1c", params={"correction_factor": 0.8})
        result3 = self.platform.register_calibration_notice(third)
        self.assertEqual(result3["contradiction"]["id"], ct["id"])
        self.assertEqual(len(self.platform.open_contradictions("mri-a")), 1)

        adjudicated = self.platform.adjudicate_contradiction(
            ct["id"], {"chosen_notice_id": "n-conflict", "decided_by": "physicist-1"})
        self.assertEqual(adjudicated["status"], "resolved")
        lineage = self.platform.device_lineage("mri-a")
        self.assertEqual(lineage["intervals"][0]["status"], IV_ACTIVE)
        self.assertEqual(lineage["intervals"][0]["calibration_version"], "coil-window-v1b")
        self.assertEqual(self.platform.open_contradictions("mri-a"), [])


class PlanScopeAndGateTest(unittest.TestCase):
    def setUp(self):
        self.platform = build_platform()
        self.platform.register_calibration_notice(COIL_WINDOW)
        self.platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(self.platform)

    def test_plan_scope_only_covers_affected_center_sequence_metrics(self):
        plan = self.platform.create_plan({"trigger_notice_id": "n1"})
        # 仅 mri-a/center-north；T1 为主序列（m4 是 T2，见下方断言）。
        self.assertEqual(plan["scope"]["center_ids"], ["center-north"])
        self.assertNotIn("m5", plan["scope"]["metric_ids"])  # mri-b 不受影响
        self.assertNotIn("m6", plan["scope"]["metric_ids"])  # 区间外采集
        unit_metrics = {u["metric_id"] for u in plan["units"]}
        self.assertIn("m1", unit_metrics)
        self.assertIn("m2", unit_metrics)
        self.assertIn("m4", unit_metrics)  # 同设备窗口内的 T2 序列也受影响
        # 同意范围不足的 p3 指标：在范围内但被排除，无执行单元。
        self.assertIn("m3", plan["scope"]["metric_ids"])
        self.assertNotIn("m3", unit_metrics)
        excluded = {(e.get("metric_id"), e["reason"]) for e in plan["excluded"] if "metric_id" in e}
        self.assertIn(("m3", "consent_scope_insufficient"), excluded)
        self.assertEqual(plan["status"], PLAN_AWAITING_APPROVAL)
        self.assertIn("a-frozen", plan["affected_analyses"])
        self.assertIn("a-live", plan["affected_analyses"])

    def test_same_notice_cannot_be_queued_twice(self):
        self.platform.create_plan({"trigger_notice_id": "n1"})
        with self.assertRaises(DomainError) as error:
            self.platform.create_plan({"trigger_notice_id": "n1"})
        self.assertEqual(error.exception.code, "plan_already_queued")

    def test_duplicate_notice_cannot_trigger_plan(self):
        dup = dict(COIL_WINDOW, notice_id="n1-dup-attempt")
        # 不同 notice_id 但同参数同时点 -> duplicate
        result = self.platform.register_calibration_notice(dup)
        self.assertTrue(result["duplicate"])
        with self.assertRaises(DomainError) as error:
            self.platform.create_plan({"trigger_notice_id": "n1-dup-attempt"})
        self.assertEqual(error.exception.code, "duplicate_notice")

    def test_requires_both_approvals(self):
        plan = self.platform.create_plan({"trigger_notice_id": "n1"})
        with self.assertRaises(DomainError) as error:
            self.platform.execute_plan(plan["plan_id"])
        self.assertEqual(error.exception.code, "plan_not_approved")
        self.platform.approve_plan(plan["plan_id"],
                                   {"role": "qc", "approver": "qc-head", "evidence": "漂移 5%"})
        with self.assertRaises(DomainError) as error:
            self.platform.execute_plan(plan["plan_id"])
        self.assertEqual(error.exception.code, "plan_not_approved")
        self.platform.approve_plan(plan["plan_id"],
                                   {"role": "statistician", "approver": "stat-head",
                                    "impact_note": "阶段比较效应变化在可接受范围"})
        executed = self.platform.execute_plan(plan["plan_id"])
        self.assertEqual(executed["status"], PLAN_COMPLETED)


class ExecutionAndPersistenceTest(unittest.TestCase):
    def _approved_plan(self, platform):
        plan = platform.create_plan({"trigger_notice_id": "n1"})
        platform.approve_plan(plan["plan_id"], {"role": "qc", "approver": "qc"})
        platform.approve_plan(plan["plan_id"], {"role": "statistician", "approver": "stat"})
        return plan["plan_id"]

    def test_frozen_analysis_keeps_snapshot_and_gets_erratum_candidate(self):
        platform = build_platform()
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        plan_id = self._approved_plan(platform)
        platform.execute_plan(plan_id)

        analysis = platform.get_analysis("a-frozen")
        self.assertEqual(analysis["status"], "frozen")
        # 已发表快照保留原值。
        self.assertEqual(analysis["snapshot"]["metric_values"]["m1"], 100.0)
        self.assertEqual(len(analysis["errata"]), 1)
        erratum = analysis["errata"][0]
        self.assertEqual(erratum["status"], "candidate")
        self.assertEqual(erratum["new_versions"]["m1"]["value"], 105.0)
        self.assertEqual(erratum["new_versions"]["m1"]["calibration_version"],
                         "coil-window-corrected-v1")
        # 勘误候选保留的快照即原快照。
        self.assertEqual(erratum["preserved_snapshot"]["metric_values"]["m2"], 110.0)

        published = platform.publish_erratum(erratum["erratum_id"], {"published_by": "editor"})
        self.assertEqual(published["status"], "published")

    def test_unfrozen_analysis_atomically_replaces_inputs(self):
        platform = build_platform()
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        plan_id = self._approved_plan(platform)
        platform.execute_plan(plan_id)

        analysis = platform.get_analysis("a-live")
        self.assertEqual(analysis["status"], "inputs_replaced")
        self.assertEqual(len(analysis["replacements_detail"]), 1)
        repl = analysis["replacements_detail"][0]
        self.assertEqual(repl["affected_metric_ids"], ["m1"])
        self.assertEqual(repl["comparison_before"]["metric_values"]["m1"], 100.0)
        self.assertEqual(repl["comparison_after"]["metric_values"]["m1"], 105.0)
        # 未受影响的 m5 保持原值。
        self.assertEqual(repl["comparison_after"]["metric_values"]["m5"], 130.0)

    def test_withdrawal_during_pending_plan_blocks_at_execution(self):
        platform = build_platform()
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        plan_id = self._approved_plan(platform)
        # 批准之后、执行之前撤回 p4：m4 不得因重算重新进入。
        platform.withdraw_participant("p4")
        plan = platform.execute_plan(plan_id)
        self.assertEqual(plan["status"], PLAN_COMPLETED)
        m4_unit = next(u for u in plan["units"] if u["metric_id"] == "m4")
        self.assertEqual(m4_unit["status"], UNIT_SKIPPED)
        self.assertEqual(m4_unit["skipped_reason"], "withdrawn")
        self.assertEqual(platform.explain_metric("m4")["reason_unchanged"], "withdrawn")

    def test_resumes_from_last_completed_unit_after_failure(self):
        calls = []

        def flaky(metric, scan, interval, params):
            calls.append(metric["metric_id"])
            if metric["metric_id"] == "m4":
                raise RuntimeError("pipeline worker lost")
            return metric["current"]["value"] * params["correction_factor"]

        platform = build_platform(recompute_fn=flaky)
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        plan_id = self._approved_plan(platform)

        paused = platform.execute_plan(plan_id)
        self.assertEqual(paused["status"], "paused")
        checkpoint = paused["checkpoint"]["completed_units"]
        self.assertNotIn(next(u["unit_id"] for u in paused["units"] if u["metric_id"] == "m4"),
                         checkpoint)
        done_before = set(checkpoint)

        # 恢复：用稳定重算函数重放，已完成单元不重新计算。
        platform.recompute_fn = lambda metric, scan, interval, params: (
            metric["current"]["value"] * params["correction_factor"]
        )
        resumed = platform.execute_plan(plan_id)
        self.assertEqual(resumed["status"], PLAN_COMPLETED)
        self.assertEqual(calls, ["m1", "m2", "m4"])  # m4 前失败，恢复只重算它
        for unit in resumed["units"]:
            if unit["metric_id"] != "m3":
                self.assertEqual(unit["status"], UNIT_DONE)
        self.assertEqual(set(resumed["checkpoint"]["completed_units"]), done_before | {
            next(u["unit_id"] for u in resumed["units"] if u["metric_id"] == "m4")
        })

    def test_state_persists_across_restart(self):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tmp.close()
        try:
            platform = Platform(Store(tmp.name))
            platform.register_device({"device_id": "mri-a", "center_id": "c"})
            platform.register_calibration_notice({
                "notice_id": "n1", "device_id": "mri-a",
                "valid_from": "2025-03-01T00:00:00Z",
                "calibration_version": "v1", "params": {"correction_factor": 1.0}})
            del platform

            restored = Platform(Store(tmp.name))
            lineage = restored.device_lineage("mri-a")
            self.assertEqual(lineage["intervals"][0]["calibration_version"], "v1")
        finally:
            os.unlink(tmp.name)


class ExplainTest(unittest.TestCase):
    def test_explain_recomputed_and_unchanged_metrics(self):
        platform = build_platform()
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        plan = platform.create_plan({"trigger_notice_id": "n1"})
        platform.approve_plan(plan["plan_id"], {"role": "qc", "approver": "qc"})
        platform.approve_plan(plan["plan_id"], {"role": "statistician", "approver": "stat"})
        platform.execute_plan(plan["plan_id"])

        recomputed = platform.explain_metric("m1")
        self.assertTrue(recomputed["recomputed"])
        self.assertEqual(recomputed["current"]["value"], 105.0)
        self.assertEqual(recomputed["calibration_used"]["calibration_version"], "coil-window-corrected-v1")
        frozen_impact, live_impact = sorted(
            recomputed["stage_comparison_impacts"], key=lambda i: i["analysis_id"])
        self.assertTrue(frozen_impact["snapshot_preserved"])
        self.assertIsNotNone(frozen_impact["erratum_candidate_id"])
        self.assertTrue(live_impact["atomic_replaced"])
        self.assertEqual(live_impact["comparison_after"]["metric_values"]["m1"], 105.0)

        # 另一台设备的指标：未受影响、保持原值。
        unaffected = platform.explain_metric("m5")
        self.assertFalse(unaffected["recomputed"])
        self.assertEqual(unaffected["reason_unchanged"], "unaffected")
        self.assertEqual(unaffected["current"]["value"], 130.0)

        # 同意不足的指标：解释保持原值的原因。
        gated = platform.explain_metric("m3")
        self.assertEqual(gated["reason_unchanged"], "consent_scope_insufficient")

    def test_explain_while_awaiting_approval(self):
        platform = build_platform()
        platform.register_calibration_notice(COIL_WINDOW)
        platform.register_calibration_notice(CORRECTION)
        seed_scans_and_metrics(platform)
        platform.create_plan({"trigger_notice_id": "n1"})
        explanation = platform.explain_metric("m1")
        self.assertFalse(explanation["recomputed"])
        self.assertEqual(explanation["reason_unchanged"], "plan_awaiting_approval")


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.platform = build_platform()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.platform))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _post(self, path, body):
        request = Request(
            f"{self.base_url}{path}",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)

    def test_full_flow_over_http(self):
        status, notice = self._post("/calibration-notices", COIL_WINDOW)
        self.assertEqual(status, 200)
        self.assertFalse(notice["duplicate"])

        status, batch = self._post(
            "/scans:batch",
            {"scans": [{
                "scan_id": "s1", "visit_id": "v1", "participant_id": "p1",
                "device_id": "mri-a", "sequence": "T1",
                "acquired_at": "2025-03-10T09:00:00Z"}]},
        )
        self.assertEqual(batch, {"accepted": 1})

        status, metrics = self._post(
            "/metrics:batch",
            {"metrics": [{
                "metric_id": "m1", "visit_id": "v1", "sequence": "T1", "name": "volume",
                "source_scan_ids": ["s1"], "value": 100.0}]},
        )
        self.assertEqual(metrics, {"accepted": 1})

        status, plan = self._post("/plans", {"trigger_notice_id": "n1"})
        plan_id = plan["plan_id"]
        self._post(f"/plans/{plan_id}/approvals", {"role": "qc", "approver": "qc-head"})
        self._post(f"/plans/{plan_id}/approvals",
                   {"role": "statistician", "approver": "stat-head"})
        status, executed = self._post(f"/plans/{plan_id}/execute", {})
        self.assertEqual(executed["status"], PLAN_COMPLETED)

        with urlopen(f"{self.base_url}/metrics/m1/explain", timeout=3) as response:
            explanation = json.load(response)
        self.assertTrue(explanation["recomputed"])
        self.assertEqual(explanation["current"]["value"], 105.0)
        self.assertEqual(explanation["calibration_used"]["calibration_version"],
                         "coil-window-corrected-v1")

    def test_domain_error_maps_to_4xx_with_code(self):
        request = Request(
            f"{self.base_url}/plans",
            data=json.dumps({"trigger_notice_id": "missing"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as error:
            urlopen(request, timeout=3)
        self.assertEqual(error.exception.code, 404)
        self.assertEqual(json.load(error.exception)["error"]["code"], "notice_not_found")
        error.exception.close()


if __name__ == "__main__":
    unittest.main()

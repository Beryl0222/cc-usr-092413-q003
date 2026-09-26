"""校准漂移重算的端到端契约测试。

覆盖领域规则：版本谱系、通告去重、矛盾暂停与裁决、按采集时点圈定、
同意/撤回过滤、双门禁、检查点恢复（含跨进程持久化）、冻结快照+勘误、
原子替换、阶段比较影响，以及 HTTP 接口与指标解释。
"""

import json
import os
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from recalc import (
    RecalcError,
    RecalcStore,
    approve_plan,
    create_plan,
    device_lineage,
    explain_metric,
    register_comparison,
    register_consent,
    register_device,
    register_metric,
    register_site,
    register_visit,
    register_withdrawal,
    resolve_calibration_conflict,
    run_plan,
    submit_calibration_notice,
)
from service import build_server

CORRECTED = {"gain": 1.1, "bias": 5}


def seed_world():
    """构造两中心、四参与者、跨区间派生指标的标准场景。"""
    store = RecalcStore()
    register_site(store, {"site_id": "S1"})
    register_site(store, {"site_id": "S2"})
    register_device(store, {"device_id": "D1", "site_id": "S1"})
    register_device(store, {"device_id": "D2", "site_id": "S2"})

    # 设备 D1 的基线校准版本（直接注入存储，模拟既有谱系）。
    store.data["calibrations"].append(
        {
            "cal_id": "cal-base",
            "device_id": "D1",
            "version": "v1",
            "params": {"gain": 1.0, "bias": 0},
            "params_hash": "base",
            "valid_from": "2023-01-01T00:00:00",
            "valid_to": None,
            "status": "active",
            "source_notice_id": None,
        }
    )

    register_consent(
        store,
        {"participant_id": "P1", "valid_from": "2024-01-01T00:00:00", "valid_to": "2026-01-01T00:00:00"},
    )
    # P2 的同意在错误区间开始后不久到期：V2 采集时同意不足。
    register_consent(
        store,
        {"participant_id": "P2", "valid_from": "2024-01-01T00:00:00", "valid_to": "2024-04-01T00:00:00"},
    )
    register_consent(
        store,
        {"participant_id": "P3", "valid_from": "2024-01-01T00:00:00", "valid_to": "2026-01-01T00:00:00"},
    )
    register_withdrawal(store, {"participant_id": "P3", "withdrawn_at": "2024-05-01T00:00:00"})
    register_consent(
        store,
        {"participant_id": "P4", "valid_from": "2024-01-01T00:00:00", "valid_to": "2026-01-01T00:00:00"},
    )
    register_consent(
        store,
        {"participant_id": "P5", "valid_from": "2024-01-01T00:00:00", "valid_to": "2026-01-01T00:00:00"},
    )

    def visit(visit_id, participant, device, ts):
        return register_visit(
            store,
            {
                "visit_id": visit_id,
                "participant_id": participant,
                "site_id": "S2" if device == "D2" else "S1",
                "device_id": device,
                "acquired_at": ts,
                "sequences": [{"name": "T1"}, {"name": "T2"}],
            },
        )

    # 错误区间：2024-02-01 ~ 2024-06-01。
    visit("V1", "P1", "D1", "2024-03-01T08:00:00")   # 区间内，有效
    visit("V2", "P2", "D1", "2024-05-15T08:00:00")   # 区间内，同意不足
    visit("VW", "P3", "D1", "2024-05-20T08:00:00")   # 区间内，已撤回
    visit("V6", "P5", "D1", "2024-05-25T08:00:00")   # 区间内，执行前撤回
    visit("V3", "P1", "D1", "2024-09-01T08:00:00")   # 区间外后续访视
    visit("V4", "P4", "D2", "2024-04-01T08:00:00")   # 另一中心，完全不受影响

    def metric(metric_id, visit_id, status="draft", raw=None, derived=None):
        payload = {
            "metric_id": metric_id,
            "participant_id": {"V1": "P1", "V2": "P2", "VW": "P3", "V6": "P5", "V3": "P1", "V4": "P4"}[visit_id],
            "visit_id": visit_id,
            "sequence": "T1",
            "name": "volume",
            "status": status,
        }
        if raw is not None:
            payload["raw_signal"] = raw
        if derived:
            payload["derived_from"] = derived
        return register_metric(store, payload)

    metric("m1", "V1", raw=100)                    # 草稿叶子 → 原子替换，新值 115
    metric("m_pub", "V1", status="published", raw=110)  # 已发表 → 勘误候选，新值 126
    metric("m2", "V2", raw=200)                    # 同意不足 → 阻断
    metric("mw", "VW", raw=210)                    # 已撤回 → 阻断
    metric("m6", "V6", raw=300)                    # 执行前撤回 → 运行时跳过
    metric("md", "V3", derived=["m1"])             # 跨访视派生 → 纳入，新值 115
    metric("mb", "V3", derived=["m2"])             # 上游被排除 → 阻断挂起
    metric("m4", "V4", status="frozen", raw=50)    # 他中心冻结 → 原封不动

    register_comparison(
        store,
        {"comparison_id": "cmp_draft", "life_stage_pair": ["premenopause", "perimenopause"],
         "metric_ids": ["m1", "md"], "status": "draft", "value": 100.0},
    )
    register_comparison(
        store,
        {"comparison_id": "cmp_pub", "life_stage_pair": ["perimenopause", "postmenopause"],
         "metric_ids": ["m_pub"], "status": "published", "value": 110.0},
    )
    register_comparison(
        store,
        {"comparison_id": "cmp_blocked", "life_stage_pair": ["premenopause", "perimenopause"],
         "metric_ids": ["m1", "m2"], "status": "draft", "value": 150.0},
    )
    register_comparison(
        store,
        {"comparison_id": "cmp_s2", "life_stage_pair": ["premenopause", "perimenopause"],
         "metric_ids": ["m4"], "status": "frozen", "value": 50.0},
    )
    return store


def make_notice(store, params=CORRECTED, valid_from="2024-02-01T00:00:00",
                valid_to="2024-06-01T00:00:00"):
    return submit_calibration_notice(
        store,
        {"device_id": "D1", "valid_from": valid_from, "valid_to": valid_to, "params": params},
    )


def dual_approve(store, plan_id):
    approve_plan(store, plan_id, "qc", "qc-lead", evidence={"phantom_drift_ppm": 4.2})
    approve_plan(store, plan_id, "statistician", "stat-lead", evidence={"affected_analyses": 2})


def by_id(items, key, value):
    return next(item for item in items if item[key] == value)


class LineageAndNoticeTest(unittest.TestCase):
    def setUp(self):
        self.store = seed_world()

    def test_duplicate_notice_is_deduplicated(self):
        first = make_notice(self.store)
        second = make_notice(self.store)
        self.assertEqual(first["status"], "queued")
        self.assertEqual(second["status"], "deduplicated")
        self.assertEqual(second["duplicate_of"], first["notice_id"])
        with self.assertRaises(RecalcError):
            create_plan(self.store, {"notice_id": second["notice_id"]})

    def test_conflicting_params_quarantine_and_resume_after_ruling(self):
        first = make_notice(self.store, params={"gain": 1.1, "bias": 5})
        # 首条通告已生成计划并完成部分审批。
        plan = create_plan(self.store, {"notice_id": first["notice_id"]})
        approve_plan(self.store, plan["plan_id"], "qc", "qc-lead")

        # 同一区间收到矛盾参数：双方隔离，计划挂起。
        conflict = make_notice(self.store, params={"gain": 1.2, "bias": 9})
        self.assertEqual(conflict["status"], "quarantined")
        self.assertEqual(conflict["conflict_with"], [first["notice_id"]])
        suspended = next(p for p in self.store.data["plans"] if p["plan_id"] == plan["plan_id"])
        self.assertEqual(suspended["status"], "suspended")
        with self.assertRaises(RecalcError):
            run_plan(self.store, plan["plan_id"])
        # 隔离期间不再接收新通告。
        with self.assertRaises(RecalcError):
            make_notice(self.store, params={"gain": 1.3, "bias": 1})

        # 裁决：第二版胜出。旧计划基于败方参数而作废，
        # 系统基于胜方通告生成新计划，审批须重新进行。
        ruling = resolve_calibration_conflict(
            self.store,
            {"device_id": "D1", "valid_from": "2024-02-01T00:00:00",
             "winning_notice_id": conflict["notice_id"]},
        )
        self.assertEqual(ruling["loser_notice_ids"], [first["notice_id"]])
        self.assertEqual(ruling["cancelled_plan_ids"], [plan["plan_id"]])
        self.assertIsNotNone(ruling["new_plan_id"])
        cancelled = next(p for p in self.store.data["plans"] if p["plan_id"] == plan["plan_id"])
        self.assertEqual(cancelled["status"], "cancelled_conflict_lost")
        new_plan = next(
            p for p in self.store.data["plans"] if p["plan_id"] == ruling["new_plan_id"]
        )
        self.assertEqual(new_plan["status"], "proposed")
        self.assertEqual(new_plan["cal_id_after"], ruling["cal_id"])

    def test_lineage_closes_previous_interval(self):
        notice = make_notice(self.store)
        create_plan(self.store, {"notice_id": notice["notice_id"]})
        lineage = device_lineage(self.store, "D1")
        versions = {c["version"]: c for c in lineage["calibrations"]}
        self.assertEqual(versions["v1"]["status"], "superseded")
        self.assertEqual(versions["v1"]["valid_to"], "2024-02-01T00:00:00")
        self.assertEqual(versions["v2"]["status"], "active")
        self.assertEqual(versions["v2"]["params"], CORRECTED)
        self.assertEqual(len(lineage["notices"]), 1)


class PlanScopingTest(unittest.TestCase):
    def setUp(self):
        self.store = seed_world()
        notice = make_notice(self.store)
        self.plan = create_plan(self.store, {"notice_id": notice["notice_id"]})

    def test_plan_covers_only_affected_center_sequences_and_closure(self):
        unit_visits = {u["visit_id"] for u in self.plan["units"]}
        # V1 区间内；V3 区间外但派生自受影响指标；V6 区间内有效。
        self.assertEqual(unit_visits, {"V1", "V6", "V3"})
        v1 = by_id(self.plan["units"], "visit_id", "V1")
        self.assertEqual(set(v1["metric_ids"]), {"m1", "m_pub"})
        v3 = by_id(self.plan["units"], "visit_id", "V3")
        self.assertEqual(v3["metric_ids"], ["md"])
        self.assertFalse(v3["in_calibration_interval"])
        # 只覆盖受影响中心 S1。
        self.assertTrue(all(u["site_id"] == "S1" for u in self.plan["units"]))
        self.assertEqual(self.plan["sequences"], ["T1", "T2"])

    def test_excluded_visits_and_blocked_metrics(self):
        reasons = {e["visit_id"]: e["reason"] for e in self.plan["excluded_visits"]}
        self.assertEqual(reasons["V2"], "consent_out_of_scope")
        self.assertEqual(reasons["VW"], "participant_withdrawn")
        blocked = {b["metric_id"]: b["reason"] for b in self.plan["blocked_metrics"]}
        self.assertEqual(blocked["m2"], "consent_out_of_scope")
        self.assertEqual(blocked["mw"], "participant_withdrawn")
        self.assertEqual(blocked["mb"], "upstream_source_excluded")

    def test_duplicate_plan_creation_rejected(self):
        with self.assertRaises(RecalcError):
            create_plan(self.store, {"notice_id": self.plan["trigger_notice_id"]})


class ApprovalAndExecutionTest(unittest.TestCase):
    def setUp(self):
        self.store = seed_world()
        notice = make_notice(self.store)
        self.plan_id = create_plan(self.store, {"notice_id": notice["notice_id"]})["plan_id"]

    def test_requires_both_approvals_in_order(self):
        with self.assertRaises(RecalcError):
            run_plan(self.store, self.plan_id)
        # 统计批准不得早于质控确认。
        with self.assertRaises(RecalcError):
            approve_plan(self.store, self.plan_id, "statistician", "stat-lead")
        approve_plan(self.store, self.plan_id, "qc", "qc-lead")
        plan = approve_plan(self.store, self.plan_id, "statistician", "stat-lead")
        self.assertEqual(plan["status"], "approved")

    def test_draft_replaced_frozen_errata_other_center_untouched(self):
        dual_approve(self.store, self.plan_id)
        result = run_plan(self.store, self.plan_id)
        self.assertEqual(result["status"], "completed")
        metrics = {m["metric_id"]: m for m in self.store.data["metrics"]}

        # 草稿原子替换。
        self.assertEqual(metrics["m1"]["value"], 115.0)
        self.assertIsNotNone(metrics["m1"]["replaced_by_unit"])
        # 跨访视派生采用已替换的上游新值。
        self.assertEqual(metrics["md"]["value"], 115.0)
        # 已发表：原快照保留，勘误候选连接新结果。
        self.assertEqual(metrics["m_pub"]["value"], 110)
        candidate = metrics["m_pub"]["errata_candidate"]
        self.assertEqual(candidate["old_snapshot"]["value"], 110)
        self.assertEqual(candidate["new_value"], 126.0)
        self.assertEqual(candidate["new_cal_version"], "v2")
        # 阻断指标保持原值。
        self.assertEqual(metrics["m2"]["value"], 200)
        self.assertIsNone(metrics["m2"]["errata_candidate"])
        self.assertEqual(metrics["mb"]["value"], None)
        # 他中心冻结结果完全不变。
        self.assertEqual(metrics["m4"]["value"], 50)
        self.assertIsNone(metrics["m4"]["errata_candidate"])
        self.assertIsNone(metrics["m4"]["replaced_by_unit"])

    def test_runtime_consent_gate_skips_unit(self):
        # 计划生成后 P5 撤回：执行时该单元跳过，数据不回流。
        dual_approve(self.store, self.plan_id)
        register_withdrawal(self.store, {"participant_id": "P5", "withdrawn_at": "2024-05-24T00:00:00"})
        run_plan(self.store, self.plan_id)
        metrics = {m["metric_id"]: m for m in self.store.data["metrics"]}
        self.assertEqual(metrics["m6"]["value"], 300)
        self.assertIsNone(metrics["m6"]["replaced_by_unit"])
        unit_v6 = by_id(self.store.data["plans"][0]["units"], "visit_id", "V6")
        self.assertEqual(unit_v6["status"], "done")
        self.assertEqual(unit_v6["skipped"], "participant_withdrawn")

    def test_checkpoint_resume_after_failure(self):
        dual_approve(self.store, self.plan_id)
        interrupted = run_plan(self.store, self.plan_id, fail_after=1)
        self.assertEqual(interrupted["status"], "interrupted")
        self.assertEqual(interrupted["completed_count"], 1)
        done_unit = next(u for u in interrupted["units"] if u["status"] == "done")

        # 恢复执行：已完成单元不重算，其余继续。
        resumed = run_plan(self.store, self.plan_id)
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["completed_count"], 3)
        self.assertTrue(all(u["status"] == "done" for u in resumed["units"]))
        # 第一个单元只产出一次结果（未重复执行）。
        self.assertEqual(len(done_unit["outcomes"]), 2)

    def test_resume_across_process_restart_with_persistent_store(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            store = RecalcStore(path)
            seed = seed_world()
            seed.path = path  # 让同一内存数据落到指定快照文件
            seed.save()
            store = RecalcStore(path)

            notice = make_notice(store)
            plan_id = create_plan(store, {"notice_id": notice["notice_id"]})["plan_id"]
            dual_approve(store, plan_id)
            run_plan(store, plan_id, fail_after=1)

            # 模拟进程重启：重新从快照加载后继续。
            rebooted = RecalcStore(path)
            resumed = run_plan(rebooted, plan_id)
            self.assertEqual(resumed["status"], "completed")
            metrics = {m["metric_id"]: m for m in rebooted.data["metrics"]}
            self.assertEqual(metrics["m1"]["value"], 115.0)
            self.assertEqual(metrics["md"]["value"], 115.0)
            self.assertEqual(metrics["m4"]["value"], 50)
        finally:
            os.unlink(path)

    def test_life_stage_comparison_outcomes(self):
        dual_approve(self.store, self.plan_id)
        run_plan(self.store, self.plan_id)
        impacts = {
            i["comparison_id"]: i
            for i in self.store.data["plans"][0]["comparison_impacts"]
        }
        # 草稿比较原子替换。
        self.assertEqual(impacts["cmp_draft"]["action"], "atomic_replaced")
        self.assertEqual(impacts["cmp_draft"]["new_value"], 115.0)
        # 已发表比较走勘误候选。
        self.assertEqual(impacts["cmp_pub"]["action"], "errata_candidate")
        self.assertEqual(impacts["cmp_pub"]["new_value"], 126.0)
        # 混用被阻断指标的比较挂起，不静默混合。
        self.assertEqual(impacts["cmp_blocked"]["action"], "held_blocked_source")
        self.assertIsNone(impacts["cmp_blocked"]["new_value"])
        self.assertIn("m2", impacts["cmp_blocked"]["blocked_metric_ids"])
        # 他中心比较不受影响，不出现在影响清单。
        self.assertNotIn("cmp_s2", impacts)
        comparisons = {c["comparison_id"]: c for c in self.store.data["comparisons"]}
        self.assertEqual(comparisons["cmp_s2"]["value"], 50.0)
        self.assertIsNone(comparisons["cmp_s2"]["errata_candidate"])


class ExplainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = seed_world()
        notice = make_notice(cls.store)
        plan_id = create_plan(cls.store, {"notice_id": notice["notice_id"]})["plan_id"]
        dual_approve(cls.store, plan_id)
        run_plan(cls.store, plan_id)
        cls.plan_id = plan_id

    def test_explain_recomputed_draft(self):
        info = explain_metric(self.store, "m1")
        self.assertTrue(info["recomputed"])
        self.assertEqual(info["current_value"], 115.0)
        self.assertEqual(info["current_cal_version"], "v2")
        self.assertIn("原子替换", info["reason"])
        cmp_impact = info["comparison_impacts"][0]
        self.assertEqual(cmp_impact["comparison_id"], "cmp_draft")
        self.assertEqual(cmp_impact["action"], "atomic_replaced")

    def test_explain_published_errata(self):
        info = explain_metric(self.store, "m_pub")
        self.assertTrue(info["recomputed"])
        self.assertEqual(info["current_value"], 110)  # 当前仍是原快照
        self.assertEqual(info["new_value"], 126.0)
        self.assertIsNotNone(info["errata_candidate"])
        self.assertIn("勘误候选", info["reason"])
        cmp_impact = next(
            c for c in info["comparison_impacts"] if c["comparison_id"] == "cmp_pub"
        )
        self.assertEqual(cmp_impact["action"], "errata_candidate")

    def test_explain_consent_and_upstream_block(self):
        info_m2 = explain_metric(self.store, "m2")
        self.assertFalse(info_m2["recomputed"])
        self.assertEqual(info_m2["exclusion_code"], "consent_out_of_scope")
        self.assertEqual(info_m2["current_cal_version"], "v1")

        info_mb = explain_metric(self.store, "mb")
        self.assertFalse(info_mb["recomputed"])
        self.assertEqual(info_mb["exclusion_code"], "upstream_source_excluded")
        cmp_impact = next(
            c for c in info_m2["comparison_impacts"] if c["comparison_id"] == "cmp_blocked"
        )
        self.assertEqual(cmp_impact["action"], "held_blocked_source")

    def test_explain_out_of_scope_center(self):
        info = explain_metric(self.store, "m4")
        self.assertFalse(info["recomputed"])
        self.assertEqual(info["exclusion_code"], "out_of_affected_scope")
        self.assertEqual(info["current_value"], 50)
        self.assertEqual(info["comparison_impacts"], [])


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = seed_world()
        cls.server = build_server("127.0.0.1", 0, cls.store)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def request(self, method, path, payload=None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = Request(self.base + path, data=data, method=method,
                      headers={"Content-Type": "application/json"})
        return json.load(urlopen(req, timeout=3))

    def test_full_flow_over_http(self):
        notice = self.request(
            "POST", "/calibration/notices",
            {"device_id": "D1", "valid_from": "2024-02-01T00:00:00",
             "valid_to": "2024-06-01T00:00:00", "params": CORRECTED},
        )
        self.assertEqual(notice["status"], "queued")

        # 重复通告不重复排队。
        dup = self.request(
            "POST", "/calibration/notices",
            {"device_id": "D1", "valid_from": "2024-02-01T00:00:00",
             "valid_to": "2024-06-01T00:00:00", "params": CORRECTED},
        )
        self.assertEqual(dup["status"], "deduplicated")

        plan = self.request("POST", "/recalc/plans", {"notice_id": notice["notice_id"]})
        self.assertEqual(plan["site_id"], "S1")
        self.request("POST", f"/recalc/plans/{plan['plan_id']}/approvals",
                     {"role": "qc", "approver": "qc-lead"})
        self.request("POST", f"/recalc/plans/{plan['plan_id']}/approvals",
                     {"role": "statistician", "approver": "stat-lead"})
        result = self.request("POST", f"/recalc/plans/{plan['plan_id']}/run", {})
        self.assertEqual(result["status"], "completed")

        plan_view = self.request("GET", f"/recalc/plans/{plan['plan_id']}")
        self.assertEqual(plan_view["unit_count"], 3)

        info = self.request("GET", "/metrics/m1/explain")
        self.assertEqual(info["current_value"], 115.0)
        self.assertEqual(info["current_cal_version"], "v2")
        self.assertTrue(info["recomputed"])

        lineage = self.request("GET", "/devices/D1/lineage")
        self.assertEqual([c["version"] for c in lineage["calibrations"]], ["v1", "v2"])

    def test_bad_request_returns_400(self):
        with self.assertRaises(HTTPError) as error:
            self.request("POST", "/calibration/notices", {"device_id": "D1"})
        self.assertEqual(error.exception.code, 400)
        body = json.load(error.exception)
        self.assertIn("error", body)
        error.exception.close()


if __name__ == "__main__":
    unittest.main()

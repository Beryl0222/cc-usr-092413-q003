"""扫描设备校准谱系与派生指标重算平台。

领域职责：

* 维护扫描设备的校准区间版本谱系（线圈更换等事件形成区间链）；
* 重复校准通告幂等去重，同一区间收到矛盾参数时暂停并等待裁决；
* 按采集时点判定受影响访视/序列/派生指标，生成限定范围的重算计划；
* 计划须经质控（漂移证据）与统计（分析影响）双批准后才能执行；
* 已冻结（锁库后发表）分析保留原快照，另建勘误候选连接新结果；
  未冻结分析在其全部受影响输入就绪后原子替换；
* 撤回参与者或同意范围不足的数据在计划与执行两个阶段都被闸门拦截；
* 执行按单元提交检查点，故障恢复后从最后完成单元继续；
* 指标解释接口说明是否重算、保持原值的原因、采用的校准版本与
  对人生阶段比较的影响。

状态以 JSON 文件原子落盘，所有写操作在同一把锁内完成。
"""

from __future__ import annotations

import copy
import json
import os
import threading
import uuid
from datetime import datetime, timezone

# 重算所要求的同意范围；不足则该访视数据不得因重算重新进入。
RECOMPUTE_CONSENT_SCOPE = "recompute"

# 计划单元状态。
UNIT_PENDING = "pending"
UNIT_DONE = "done"
UNIT_SKIPPED = "skipped"

# 计划状态。
PLAN_DRAFT = "draft"
PLAN_AWAITING_APPROVAL = "awaiting_approval"
PLAN_APPROVED = "approved"
PLAN_RUNNING = "running"
PLAN_PAUSED = "paused"
PLAN_COMPLETED = "completed"
PLAN_REJECTED = "rejected"
PLAN_BLOCKED = "blocked"

# 校准区间状态。
IV_ACTIVE = "active"
IV_SUPERSEDED = "superseded"
IV_PENDING = "pending_adjudication"


class DomainError(Exception):
    """带稳定错误码的领域异常，HTTP 层映射为 4xx。"""

    def __init__(self, code, message, status=400, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}


def _utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _default_recompute(metric, scan, interval, params):
    """默认重算函数：按校准参数中的 correction_factor 修正数值指标。

    平台为纯逻辑组件，真实管线应注入自己的重算函数；此默认实现让
    数值结果随校准参数确定性变化，便于联调与测试。
    """
    value = metric["current"]["value"]
    factor = params.get("correction_factor")
    if isinstance(value, (int, float)) and isinstance(factor, (int, float)):
        return value * factor
    return value


class Store:
    """JSON 文件状态存储，写入采用临时文件 + 原子替换。"""

    def __init__(self, path, state=None):
        self.path = path
        self.state = state if state is not None else self._empty_state()
        if path:
            self.load()

    @staticmethod
    def _empty_state():
        return {
            "devices": {},
            "notices": {},
            "contradictions": {},
            "participants": {},
            "scans": {},
            "metrics": {},
            "analyses": {},
            "plans": {},
            "errata": {},
            "replacements": {},
        }

    def load(self):
        if self.path and os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, "r", encoding="utf-8") as handle:
                self.state = json.load(handle)

    def save(self):
        if not self.path:
            return
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self.state, handle, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)


class Platform:
    """校准谱系与重算领域服务（内存状态 + 单锁串行化）。"""

    def __init__(self, store=None, recompute_fn=None):
        self.store = store or Store(path=None)
        self.recompute_fn = recompute_fn or _default_recompute
        self._lock = threading.RLock()

    # ---------- 内部工具 ----------

    def _save(self):
        self.store.save()

    @staticmethod
    def _require(payload, fields):
        for field in fields:
            if field not in payload or payload[field] in (None, ""):
                raise DomainError("missing_field", f"缺少必填字段: {field}", 400, {"field": field})

    def _get_device(self, device_id):
        device = self.store.state["devices"].get(device_id)
        if not device:
            raise DomainError("device_not_found", f"未知扫描设备: {device_id}", 404)
        return device

    def _get_scan(self, scan_id):
        scan = self.store.state["scans"].get(scan_id)
        if not scan:
            raise DomainError("scan_not_found", f"未知扫描: {scan_id}", 404)
        return scan

    def _get_metric(self, metric_id):
        metric = self.store.state["metrics"].get(metric_id)
        if not metric:
            raise DomainError("metric_not_found", f"未知派生指标: {metric_id}", 404)
        return metric

    def _get_analysis(self, analysis_id):
        analysis = self.store.state["analyses"].get(analysis_id)
        if not analysis:
            raise DomainError("analysis_not_found", f"未知分析: {analysis_id}", 404)
        return analysis

    @staticmethod
    def _canonical_params(params):
        return json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _participant_gate(self, participant_id):
        """返回 (eligible, reason)，撤回与同意范围不足均不可重算。"""
        participant = self.store.state["participants"].get(participant_id)
        if not participant:
            return False, "participant_unknown"
        if participant.get("withdrawn"):
            return False, "withdrawn"
        if RECOMPUTE_CONSENT_SCOPE not in participant.get("consent_scopes", []):
            return False, "consent_scope_insufficient"
        return True, None

    def _device_intervals(self, device_id):
        return self.store.state["devices"][device_id]["intervals"]

    def _interval_at(self, device_id, acquired_at):
        """按采集时点定位生效校准区间（左闭右开）。"""
        for interval in self._device_intervals(device_id):
            if interval["valid_from"] <= acquired_at and (
                interval["valid_to"] is None or acquired_at < interval["valid_to"]
            ):
                return interval
        return None

    # ---------- 设备与谱系 ----------

    def register_device(self, payload):
        self._require(payload, ["device_id", "center_id"])
        device_id = payload["device_id"]
        with self._lock:
            if device_id in self.store.state["devices"]:
                raise DomainError("device_exists", f"设备已登记: {device_id}", 409)
            self.store.state["devices"][device_id] = {
                "device_id": device_id,
                "center_id": payload["center_id"],
                "model": payload.get("model"),
                "intervals": [],
                "created_at": _utcnow(),
            }
            self._save()
            return self.store.state["devices"][device_id]

    def device_lineage(self, device_id):
        with self._lock:
            device = self._get_device(device_id)
            return {
                "device_id": device_id,
                "center_id": device["center_id"],
                "intervals": sorted(device["intervals"], key=lambda iv: iv["valid_from"]),
            }

    # ---------- 校准通告 ----------

    def register_calibration_notice(self, payload):
        """登记校准通告。

        幂等规则：同设备、同生效时点、同参数视为重复通告，不重复排队；
        同一生效时点出现不同参数即构成矛盾，相关区间暂停，等待裁决。
        """
        self._require(payload, ["notice_id", "device_id", "valid_from", "params", "calibration_version"])
        notice_id = payload["notice_id"]
        device_id = payload["device_id"]
        valid_from = payload["valid_from"]
        params = payload["params"]
        cal_version = payload["calibration_version"]
        fingerprint = self._canonical_params(params)

        with self._lock:
            if notice_id in self.store.state["notices"]:
                existing = self.store.state["notices"][notice_id]
                return {"duplicate": True, "notice": existing}

            device = self._get_device(device_id)
            intervals = self._device_intervals(device_id)

            # 同一生效时点的已有区间：判断重复还是矛盾。
            same_point = next((iv for iv in intervals if iv["valid_from"] == valid_from), None)

            contradiction = None
            open_contradiction = next(
                (
                    c
                    for c in self.store.state["contradictions"].values()
                    if c["status"] == "open"
                    and c["device_id"] == device_id
                    and c["valid_from"] == valid_from
                ),
                None,
            )

            if same_point is not None and self._canonical_params(same_point["params"]) != fingerprint:
                if open_contradiction is not None:
                    # 区间已在等待裁决：新参数作为新选项并入，重复选项幂等忽略。
                    contradiction = self._add_contradiction_option(open_contradiction, payload, fingerprint)
                else:
                    contradiction = self._open_contradiction(device_id, same_point, payload)

            notice = {
                "notice_id": notice_id,
                "device_id": device_id,
                "valid_from": valid_from,
                "params": copy.deepcopy(params),
                "calibration_version": cal_version,
                "fingerprint": fingerprint,
                "received_at": payload.get("received_at", _utcnow()),
                "status": "contradicted" if contradiction else "queued",
                "interval_id": None,
                "contradiction_id": contradiction["id"] if contradiction else None,
            }

            if contradiction:
                if same_point is not None:
                    same_point["status"] = IV_PENDING
                self.store.state["notices"][notice_id] = notice
                self._save()
                return {"duplicate": False, "notice": notice, "contradiction": contradiction}

            if same_point is not None:
                # 参数完全一致：重复通告，只登记为 duplicate，不排队、不动谱系。
                notice["status"] = "duplicate"
                notice["interval_id"] = same_point["interval_id"]
                self.store.state["notices"][notice_id] = notice
                self._save()
                return {"duplicate": True, "notice": notice}

            interval = self._append_interval(device, valid_from, params, cal_version, notice_id)
            notice["interval_id"] = interval["interval_id"]
            self.store.state["notices"][notice_id] = notice
            self._save()
            return {"duplicate": False, "notice": notice, "interval": interval}

    def _append_interval(self, device, valid_from, params, cal_version, notice_id):
        """在谱系末端追加新生效区间，关闭当前开放区间。"""
        intervals = device["intervals"]
        predecessor = next((iv for iv in intervals if iv["valid_to"] is None), None)
        if predecessor is not None:
            predecessor["valid_to"] = valid_from
            if predecessor["status"] == IV_ACTIVE:
                predecessor["status"] = IV_SUPERSEDED
        interval = {
            "interval_id": _new_id("iv"),
            "device_id": device["device_id"],
            "center_id": device["center_id"],
            "valid_from": valid_from,
            "valid_to": None,
            "params": copy.deepcopy(params),
            "calibration_version": cal_version,
            "status": IV_ACTIVE,
            "predecessor_id": predecessor["interval_id"] if predecessor else None,
            "superseded_by": None,
            "created_by_notice": notice_id,
        }
        if predecessor is not None:
            predecessor["superseded_by"] = interval["interval_id"]
        intervals.append(interval)
        return interval

    def _open_contradiction(self, device_id, interval, new_payload):
        contradiction_id = _new_id("ct")
        contradiction = {
            "id": contradiction_id,
            "device_id": device_id,
            "valid_from": interval["valid_from"],
            "interval_id": interval["interval_id"],
            "options": [
                {
                    "notice_id": interval["created_by_notice"],
                    "calibration_version": interval["calibration_version"],
                    "params": copy.deepcopy(interval["params"]),
                },
                {
                    "notice_id": new_payload["notice_id"],
                    "calibration_version": new_payload["calibration_version"],
                    "params": copy.deepcopy(new_payload["params"]),
                },
            ],
            "status": "open",
            "opened_at": _utcnow(),
            "resolution": None,
        }
        self.store.state["contradictions"][contradiction_id] = contradiction
        return contradiction

    def _add_contradiction_option(self, contradiction, new_payload, fingerprint):
        known = {
            self._canonical_params(opt["params"]) for opt in contradiction["options"]
        }
        if fingerprint not in known:
            contradiction["options"].append(
                {
                    "notice_id": new_payload["notice_id"],
                    "calibration_version": new_payload["calibration_version"],
                    "params": copy.deepcopy(new_payload["params"]),
                }
            )
        return contradiction

    def _contradiction_resolved(self, device_id, valid_from):
        return any(
            c["device_id"] == device_id
            and c["valid_from"] == valid_from
            and c["status"] == "resolved"
            for c in self.store.state["contradictions"].values()
        )

    def open_contradictions(self, device_id=None):
        with self._lock:
            return [
                c
                for c in self.store.state["contradictions"].values()
                if c["status"] == "open" and (device_id is None or c["device_id"] == device_id)
            ]

    def adjudicate_contradiction(self, contradiction_id, payload):
        """裁决矛盾参数：选定一版后区间方可生效，暂停随之解除。"""
        self._require(payload, ["chosen_notice_id", "decided_by"])
        with self._lock:
            contradiction = self.store.state["contradictions"].get(contradiction_id)
            if not contradiction:
                raise DomainError("contradiction_not_found", f"未知矛盾记录: {contradiction_id}", 404)
            if contradiction["status"] != "open":
                raise DomainError("contradiction_closed", "该矛盾已裁决", 409)
            chosen = next(
                (opt for opt in contradiction["options"] if opt["notice_id"] == payload["chosen_notice_id"]),
                None,
            )
            if chosen is None:
                raise DomainError("unknown_option", "裁决所选择的通告不属于该矛盾", 400)

            device_id = contradiction["device_id"]
            device = self._get_device(device_id)
            interval = None
            if contradiction["interval_id"]:
                interval = next(
                    iv for iv in device["intervals"] if iv["interval_id"] == contradiction["interval_id"]
                )
                interval["params"] = copy.deepcopy(chosen["params"])
                interval["calibration_version"] = chosen["calibration_version"]
                interval["status"] = IV_ACTIVE
                interval["adjudicated_notice"] = chosen["notice_id"]
            else:
                interval = self._append_interval(
                    device,
                    contradiction["valid_from"],
                    chosen["params"],
                    chosen["calibration_version"],
                    chosen["notice_id"],
                )
                contradiction["interval_id"] = interval["interval_id"]

            contradiction["status"] = "resolved"
            contradiction["resolution"] = {
                "chosen_notice_id": chosen["notice_id"],
                "decided_by": payload["decided_by"],
                "decided_at": _utcnow(),
                "reason": payload.get("reason"),
            }
            for notice_id in (opt["notice_id"] for opt in contradiction["options"]):
                notice = self.store.state["notices"].get(notice_id)
                if notice is None:
                    continue
                notice["status"] = "queued" if notice_id == chosen["notice_id"] else "rejected"
                notice["interval_id"] = interval["interval_id"]
                notice["contradiction_id"] = contradiction_id
            self._save()
            return contradiction

    # ---------- 参与者 / 扫描 / 指标 / 分析登记 ----------

    def register_participants(self, participants):
        with self._lock:
            for person in participants:
                self._require(person, ["participant_id"])
                pid = person["participant_id"]
                self.store.state["participants"][pid] = {
                    "participant_id": pid,
                    "withdrawn": bool(person.get("withdrawn", False)),
                    "consent_scopes": list(person.get("consent_scopes", [])),
                }
            self._save()
            return {"accepted": len(participants)}

    def withdraw_participant(self, participant_id):
        with self._lock:
            person = self.store.state["participants"].get(participant_id)
            if not person:
                raise DomainError("participant_not_found", f"未知参与者: {participant_id}", 404)
            person["withdrawn"] = True
            self._save()
            return person

    def register_scans(self, scans):
        with self._lock:
            for scan in scans:
                self._require(
                    scan, ["scan_id", "visit_id", "participant_id", "device_id", "sequence", "acquired_at"]
                )
                self._get_device(scan["device_id"])
                sid = scan["scan_id"]
                if sid in self.store.state["scans"]:
                    raise DomainError("scan_exists", f"扫描已登记: {sid}", 409)
                self.store.state["scans"][sid] = {
                    "scan_id": sid,
                    "visit_id": scan["visit_id"],
                    "participant_id": scan["participant_id"],
                    "center_id": self.store.state["devices"][scan["device_id"]]["center_id"],
                    "device_id": scan["device_id"],
                    "sequence": scan["sequence"],
                    "acquired_at": scan["acquired_at"],
                }
            self._save()
            return {"accepted": len(scans)}

    def register_metrics(self, metrics):
        with self._lock:
            for metric in metrics:
                self._require(metric, ["metric_id", "visit_id", "sequence", "name"])
                mid = metric["metric_id"]
                if mid in self.store.state["metrics"]:
                    raise DomainError("metric_exists", f"指标已登记: {mid}", 409)
                source_scan_ids = list(metric.get("source_scan_ids", []))
                for sid in source_scan_ids:
                    self._get_scan(sid)
                cal_version = metric.get("calibration_version")
                if cal_version is None and source_scan_ids:
                    scan = self._get_scan(source_scan_ids[0])
                    interval = self._interval_at(scan["device_id"], scan["acquired_at"])
                    cal_version = interval["calibration_version"] if interval else None
                self.store.state["metrics"][mid] = {
                    "metric_id": mid,
                    "visit_id": metric["visit_id"],
                    "sequence": metric["sequence"],
                    "name": metric["name"],
                    "source_scan_ids": source_scan_ids,
                    "current": {
                        "version": 1,
                        "value": copy.deepcopy(metric.get("value")),
                        "calibration_version": cal_version,
                        "created_at": _utcnow(),
                        "recomputed": False,
                    },
                    "history": [],
                }
            self._save()
            return {"accepted": len(metrics)}

    def register_analyses(self, analyses):
        with self._lock:
            for analysis in analyses:
                self._require(analysis, ["analysis_id", "input_metric_ids", "frozen"])
                aid = analysis["analysis_id"]
                if aid in self.store.state["analyses"]:
                    raise DomainError("analysis_exists", f"分析已登记: {aid}", 409)
                metric_ids = list(analysis["input_metric_ids"])
                for mid in metric_ids:
                    self._get_metric(mid)
                frozen = bool(analysis["frozen"])
                snapshot = analysis.get("snapshot")
                if frozen and snapshot is None:
                    # 锁库发表的分析必须固化快照；缺省时从当前指标值固化。
                    snapshot = self._snapshot_inputs(metric_ids)
                record = {
                    "analysis_id": aid,
                    "title": analysis.get("title", aid),
                    "frozen": frozen,
                    "stage_comparison": {
                        "from_stage": analysis.get("from_stage"),
                        "to_stage": analysis.get("to_stage"),
                    },
                    "input_metric_ids": metric_ids,
                    "snapshot": copy.deepcopy(snapshot),
                    "status": "frozen" if frozen else "active",
                    "replacements": [],
                }
                self.store.state["analyses"][aid] = record
            self._save()
            return {"accepted": len(analyses)}

    def _snapshot_inputs(self, metric_ids):
        return {
            "metric_values": {
                mid: copy.deepcopy(self._get_metric(mid)["current"]["value"]) for mid in metric_ids
            },
            "calibration_versions": {
                mid: self._get_metric(mid)["current"]["calibration_version"] for mid in metric_ids
            },
            "captured_at": _utcnow(),
        }

    # ---------- 影响判定与重算计划 ----------

    def _affected_scans_for_interval(self, device_id, interval):
        """按采集时点（左闭右开）选出受影响扫描。"""
        affected = []
        for scan in self.store.state["scans"].values():
            if scan["device_id"] != device_id:
                continue
            if interval["valid_from"] <= scan["acquired_at"] and (
                interval["valid_to"] is None or scan["acquired_at"] < interval["valid_to"]
            ):
                affected.append(scan)
        return affected

    @staticmethod
    def _interval_fingerprint(interval):
        return f"{interval['interval_id']}:{interval['calibration_version']}"

    def create_plan(self, payload):
        """基于校准区间生成只覆盖相关中心/序列/指标的重算计划。"""
        with self._lock:
            trigger_notice_id = payload.get("trigger_notice_id")
            if trigger_notice_id:
                notice = self.store.state["notices"].get(trigger_notice_id)
                if not notice:
                    raise DomainError("notice_not_found", f"未知校准通告: {trigger_notice_id}", 404)
                if notice["status"] == "duplicate":
                    raise DomainError(
                        "duplicate_notice", "重复校准通告不会触发重算计划", 409
                    )
                if notice["status"] == "rejected":
                    raise DomainError("notice_rejected", "通告参数已在裁决中被否决", 409)
                device_id = notice["device_id"]
                interval_id = notice.get("interval_id")
                # 同一通告不得重复排队生成计划。
                for existing in self.store.state["plans"].values():
                    if trigger_notice_id in existing["trigger_notice_ids"] and existing["status"] not in (
                        PLAN_REJECTED,
                    ):
                        raise DomainError(
                            "plan_already_queued",
                            "该校准通告已排入重算计划，不得重复排队",
                            409,
                            {"plan_id": existing["plan_id"]},
                        )
            else:
                self._require(payload, ["device_id", "interval_id"])
                device_id = payload["device_id"]
                interval_id = payload["interval_id"]
                notice = None

            device = self._get_device(device_id)
            interval = next((iv for iv in device["intervals"] if iv["interval_id"] == interval_id), None)
            if interval is None:
                raise DomainError("interval_not_found", f"未知校准区间: {interval_id}", 404)
            if interval["status"] == IV_PENDING:
                raise DomainError(
                    "interval_pending_adjudication",
                    "该设备区间存在矛盾校准参数，已暂停，等待裁决后才能重算",
                    409,
                )

            # 同区间同版本参数不重复生成计划（无通告触发时的去重）。
            fingerprint = self._interval_fingerprint(interval)
            for existing in self.store.state["plans"].values():
                if existing["interval_fingerprint"] == fingerprint and existing["status"] != PLAN_REJECTED:
                    raise DomainError(
                        "plan_already_queued",
                        "该校准区间的重算计划已存在",
                        409,
                        {"plan_id": existing["plan_id"]},
                    )

            scans = self._affected_scans_for_interval(device_id, interval)
            scan_ids = {s["scan_id"] for s in scans}

            units = []
            excluded = []
            metric_ids = set()
            sequences = set()
            center_ids = set()

            # 参与者闸门（计划阶段先筛，执行阶段仍会再次校验）。
            eligible_scan_ids = set()
            for scan in scans:
                eligible, reason = self._participant_gate(scan["participant_id"])
                center_ids.add(scan["center_id"])
                if not eligible:
                    excluded.append(
                        {
                            "scan_id": scan["scan_id"],
                            "visit_id": scan["visit_id"],
                            "participant_id": scan["participant_id"],
                            "sequence": scan["sequence"],
                            "reason": reason,
                        }
                    )
                    continue
                eligible_scan_ids.add(scan["scan_id"])
                sequences.add(scan["sequence"])

            for metric in self.store.state["metrics"].values():
                touched = [sid for sid in metric["source_scan_ids"] if sid in scan_ids]
                if not touched:
                    continue
                metric_ids.add(metric["metric_id"])
                if not any(sid in eligible_scan_ids for sid in touched):
                    gate_reason = next(
                        self._participant_gate(self._get_scan(sid)["participant_id"])[1]
                        for sid in touched
                    )
                    excluded.append(
                        {
                            "metric_id": metric["metric_id"],
                            "visit_id": metric["visit_id"],
                            "sequence": metric["sequence"],
                            "reason": gate_reason or "participant_gate",
                        }
                    )
                    continue
                # 已采用目标校准版本的指标无需再算（重复通告/重放）。
                if metric["current"]["calibration_version"] == interval["calibration_version"] and metric[
                    "current"
                ].get("recomputed"):
                    excluded.append(
                        {
                            "metric_id": metric["metric_id"],
                            "visit_id": metric["visit_id"],
                            "sequence": metric["sequence"],
                            "reason": "already_current",
                        }
                    )
                    continue
                sequences.add(metric["sequence"])
                units.append(
                    {
                        "unit_id": _new_id("unit"),
                        "metric_id": metric["metric_id"],
                        "visit_id": metric["visit_id"],
                        "sequence": metric["sequence"],
                        "scan_ids": touched,
                        "status": UNIT_PENDING,
                    }
                )

            affected_analyses = [
                aid
                for aid, analysis in self.store.state["analyses"].items()
                if metric_ids.intersection(analysis["input_metric_ids"])
            ]
            # 未冻结分析的替换前基线在计划创建时冻结，
            # 执行结束后用于展示阶段比较向量的前后差异。
            analysis_baselines = {
                aid: self._comparison_vector(self.store.state["analyses"][aid])
                for aid in affected_analyses
                if not self.store.state["analyses"][aid]["frozen"]
            }

            plan_id = _new_id("plan")
            plan = {
                "plan_id": plan_id,
                "name": payload.get("name", f"重算计划 {interval['calibration_version']}"),
                "device_id": device_id,
                "interval_id": interval_id,
                "interval_fingerprint": fingerprint,
                "target_calibration_version": interval["calibration_version"],
                "trigger_notice_ids": [trigger_notice_id] if trigger_notice_id else [],
                "scope": {
                    "center_ids": sorted(center_ids),
                    "sequences": sorted(sequences),
                    "metric_ids": sorted(metric_ids),
                    "window": {"valid_from": interval["valid_from"], "valid_to": interval["valid_to"]},
                },
                "units": units,
                "excluded": excluded,
                "affected_analyses": affected_analyses,
                "analysis_baselines": analysis_baselines,
                "approvals": {
                    "qc": {"approved": False, "by": None, "at": None, "evidence": None},
                    "statistician": {"approved": False, "by": None, "at": None, "impact_note": None},
                },
                "status": PLAN_AWAITING_APPROVAL,
                "checkpoint": {"completed_units": [], "last_completed_unit": None},
                "erratum_ids": [],
                "replacement_ids": [],
                "created_at": _utcnow(),
            }
            self.store.state["plans"][plan_id] = plan
            self._save()
            return plan

    def get_plan(self, plan_id):
        with self._lock:
            plan = self.store.state["plans"].get(plan_id)
            if not plan:
                raise DomainError("plan_not_found", f"未知重算计划: {plan_id}", 404)
            return plan

    def approve_plan(self, plan_id, payload):
        """双闸门：质控确认漂移证据 + 统计负责人批准分析影响。"""
        self._require(payload, ["role", "approver"])
        role = payload["role"]
        if role not in ("qc", "statistician"):
            raise DomainError("unknown_role", "role 必须为 qc 或 statistician", 400)
        with self._lock:
            plan = self.get_plan(plan_id)
            if plan["status"] in (PLAN_REJECTED, PLAN_COMPLETED):
                raise DomainError("plan_not_approvable", f"计划状态 {plan['status']} 不可审批", 409)
            if not payload.get("approved", True):
                plan["status"] = PLAN_REJECTED
                plan["approvals"][role] = {
                    "approved": False,
                    "by": payload["approver"],
                    "at": _utcnow(),
                    "note": payload.get("note"),
                }
                self._save()
                return plan
            approval = plan["approvals"][role]
            approval["approved"] = True
            approval["by"] = payload["approver"]
            approval["at"] = _utcnow()
            if role == "qc":
                approval["evidence"] = payload.get("evidence", "漂移证据已确认")
            else:
                approval["impact_note"] = payload.get("impact_note", "分析影响已评估并批准")
            if plan["approvals"]["qc"]["approved"] and plan["approvals"]["statistician"]["approved"]:
                plan["status"] = PLAN_APPROVED
            self._save()
            return plan

    # ---------- 执行：检查点恢复、勘误候选、原子替换 ----------

    def execute_plan(self, plan_id):
        """执行重算计划。

        每个单元完成即提交检查点；若重算函数抛出异常，计划暂停，
        已完成单元保留，再次调用时从最后完成单元之后继续。
        """
        with self._lock:
            plan = self.get_plan(plan_id)
            if plan["status"] in (PLAN_AWAITING_APPROVAL, PLAN_DRAFT):
                raise DomainError("plan_not_approved", "计划尚未获得质控与统计双批准", 409)
            if plan["status"] == PLAN_REJECTED:
                raise DomainError("plan_rejected", "计划已被否决，不可执行", 409)
            if plan["status"] == PLAN_COMPLETED:
                return plan

            interval = next(
                iv
                for iv in self._device_intervals(plan["device_id"])
                if iv["interval_id"] == plan["interval_id"]
            )
            if interval["status"] == IV_PENDING:
                plan["status"] = PLAN_BLOCKED
                self._save()
                raise DomainError(
                    "interval_pending_adjudication", "区间参数出现新矛盾，计划暂停等待裁决", 409
                )

            completed = set(plan["checkpoint"]["completed_units"])
            failure = None
            for unit in plan["units"]:
                if unit["unit_id"] in completed:
                    continue
                plan["status"] = PLAN_RUNNING
                metric = self._get_metric(unit["metric_id"])
                # 执行阶段再次过参与者闸门（执行前可能发生撤回/同意收窄）。
                bad_scans = []
                for sid in unit["scan_ids"]:
                    scan = self._get_scan(sid)
                    eligible, reason = self._participant_gate(scan["participant_id"])
                    if not eligible:
                        bad_scans.append((scan, reason))
                if bad_scans:
                    unit["status"] = UNIT_SKIPPED
                    unit["skipped_reason"] = bad_scans[0][1]
                    completed.add(unit["unit_id"])
                    plan["checkpoint"]["completed_units"] = sorted(completed)
                    plan["checkpoint"]["last_completed_unit"] = unit["unit_id"]
                    self._save()
                    continue
                primary_scan = self._get_scan(unit["scan_ids"][0])
                try:
                    new_value = self.recompute_fn(metric, primary_scan, interval, interval["params"])
                except Exception as exc:  # 重算管线故障：暂停，保留检查点
                    failure = {"unit_id": unit["unit_id"], "error": str(exc)}
                    plan["status"] = PLAN_PAUSED
                    self._save()
                    return plan
                self._commit_metric_version(metric, new_value, interval, plan_id)
                unit["status"] = UNIT_DONE
                unit["new_version"] = metric["current"]["version"]
                completed.add(unit["unit_id"])
                plan["checkpoint"]["completed_units"] = sorted(completed)
                plan["checkpoint"]["last_completed_unit"] = unit["unit_id"]
                self._save()  # 单元级检查点落盘：故障恢复从此继续

            self._finalize_analyses(plan, interval)
            plan["status"] = PLAN_COMPLETED if failure is None else PLAN_PAUSED
            plan["last_failure"] = failure
            self._save()
            return plan

    def _commit_metric_version(self, metric, new_value, interval, plan_id):
        old = copy.deepcopy(metric["current"])
        metric["history"].append(old)
        metric["current"] = {
            "version": old["version"] + 1,
            "value": copy.deepcopy(new_value),
            "calibration_version": interval["calibration_version"],
            "interval_id": interval["interval_id"],
            "recomputed": True,
            "plan_id": plan_id,
            "created_at": _utcnow(),
        }

    def _finalize_analyses(self, plan, interval):
        """全部单元就绪后处理分析：冻结→勘误候选；未冻结→原子替换。"""
        done_metric_ids = {
            u["metric_id"] for u in plan["units"] if u["status"] == UNIT_DONE
        }
        for aid in plan["affected_analyses"]:
            analysis = self._get_analysis(aid)
            affected_inputs = [mid for mid in analysis["input_metric_ids"] if mid in done_metric_ids]
            if not affected_inputs:
                continue
            if aid in plan.get("erratum_ids", []) or aid in plan.get("replacement_ids", []):
                continue  # 恢复执行时不重复建勘误/替换
            if analysis["frozen"]:
                # 已发表分析：原快照一字不动，另建勘误候选连接新结果。
                erratum_id = _new_id("erratum")
                erratum = {
                    "erratum_id": erratum_id,
                    "analysis_id": aid,
                    "plan_id": plan["plan_id"],
                    "status": "candidate",
                    "new_versions": {
                        mid: {
                            "version": self._get_metric(mid)["current"]["version"],
                            "value": copy.deepcopy(self._get_metric(mid)["current"]["value"]),
                            "calibration_version": self._get_metric(mid)["current"]["calibration_version"],
                        }
                        for mid in affected_inputs
                    },
                    "preserved_snapshot": copy.deepcopy(analysis["snapshot"]),
                    "created_at": _utcnow(),
                }
                self.store.state["errata"][erratum_id] = erratum
                plan["erratum_ids"].append(erratum_id)
            else:
                # 未冻结分析：使用计划创建时冻结的基线，随后一步原子切换输入。
                before = copy.deepcopy(plan.get("analysis_baselines", {}).get(aid))
                replacement = {
                    "replacement_id": _new_id("repl"),
                    "analysis_id": aid,
                    "plan_id": plan["plan_id"],
                    "replaced_at": _utcnow(),
                    "affected_metric_ids": affected_inputs,
                    "versions_before": {
                        mid: self._get_metric(mid)["history"][-1]["version"] for mid in affected_inputs
                    },
                    "versions_after": {
                        mid: self._get_metric(mid)["current"]["version"] for mid in affected_inputs
                    },
                    "comparison_before": before,
                    "comparison_after": None,
                }
                # 原子替换点：输入引用与比较向量在同一把锁内切换。
                analysis["input_metric_ids"] = list(analysis["input_metric_ids"])
                analysis["status"] = "inputs_replaced"
                analysis["replacements"].append(replacement["replacement_id"])
                replacement["comparison_after"] = self._comparison_vector(analysis)
                self.store.state["analyses"][aid] = analysis
                self.store.state.setdefault("replacements", {})[replacement["replacement_id"]] = replacement
                plan["replacement_ids"].append(replacement["replacement_id"])

    def _comparison_vector(self, analysis):
        """人生阶段比较所用的指标向量（from_stage→to_stage 的可比较快照）。"""
        return {
            "from_stage": analysis["stage_comparison"]["from_stage"],
            "to_stage": analysis["stage_comparison"]["to_stage"],
            "metric_values": {
                mid: copy.deepcopy(self._get_metric(mid)["current"]["value"])
                for mid in analysis["input_metric_ids"]
            },
            "calibration_versions": {
                mid: self._get_metric(mid)["current"]["calibration_version"]
                for mid in analysis["input_metric_ids"]
            },
        }

    def publish_erratum(self, erratum_id, payload):
        with self._lock:
            erratum = self.store.state["errata"].get(erratum_id)
            if not erratum:
                raise DomainError("erratum_not_found", f"未知勘误候选: {erratum_id}", 404)
            if erratum["status"] != "candidate":
                raise DomainError("erratum_not_candidate", "勘误候选已处理", 409)
            self._require(payload, ["published_by"])
            erratum["status"] = "published"
            erratum["published_by"] = payload["published_by"]
            erratum["published_at"] = _utcnow()
            self._save()
            return erratum

    # ---------- 解释查询 ----------

    def explain_metric(self, metric_id):
        """解释指标现状：是否重算、为何保持原值、采用哪版校准、阶段比较影响。"""
        with self._lock:
            metric = self._get_metric(metric_id)
            current = metric["current"]
            calibration = None
            affected_window = False
            contradiction_open = False

            for sid in metric["source_scan_ids"]:
                scan = self.store.state["scans"].get(sid)
                if not scan:
                    continue
                interval = self._interval_at(scan["device_id"], scan["acquired_at"])
                if interval is None:
                    continue
                affected_window = True
                calibration = {
                    "device_id": scan["device_id"],
                    "interval_id": interval["interval_id"],
                    "calibration_version": interval["calibration_version"],
                    "valid_from": interval["valid_from"],
                    "valid_to": interval["valid_to"],
                    "params": copy.deepcopy(interval["params"]),
                    "status": interval["status"],
                }
                if interval["status"] == IV_PENDING:
                    contradiction_open = True

            # 计划与排除证据。
            plan_refs = []
            excluded_reason = None
            for plan in self.store.state["plans"].values():
                if metric_id in plan["scope"]["metric_ids"]:
                    plan_refs.append(plan["plan_id"])
                for item in plan["excluded"]:
                    if item.get("metric_id") == metric_id:
                        excluded_reason = item["reason"]

            gate_reasons = []
            for sid in metric["source_scan_ids"]:
                scan = self.store.state["scans"].get(sid)
                if scan:
                    _eligible, reason = self._participant_gate(scan["participant_id"])
                    if reason:
                        gate_reasons.append(reason)

            if current.get("recomputed"):
                status = "recomputed"
                reason_unchanged = None
            elif contradiction_open:
                status = "original"
                reason_unchanged = "contradiction_pending_adjudication"
            elif gate_reasons:
                status = "original"
                reason_unchanged = gate_reasons[0]
            elif excluded_reason == "already_current":
                status = "original"
                reason_unchanged = "already_current"
            elif plan_refs:
                status = "original"
                plan = self.store.state["plans"][plan_refs[-1]]
                reason_unchanged = f"plan_{plan['status']}"
            elif affected_window:
                status = "original"
                reason_unchanged = "affected_but_no_plan"
            else:
                status = "original"
                reason_unchanged = "unaffected"

            impacts = []
            for aid, analysis in self.store.state["analyses"].items():
                if metric_id not in analysis["input_metric_ids"]:
                    continue
                entry = {
                    "analysis_id": aid,
                    "frozen": analysis["frozen"],
                    "stage_comparison": copy.deepcopy(analysis["stage_comparison"]),
                }
                if analysis["frozen"]:
                    erratum = next(
                        (
                            e
                            for e in self.store.state["errata"].values()
                            if e["analysis_id"] == aid and metric_id in e["new_versions"]
                        ),
                        None,
                    )
                    entry["snapshot_preserved"] = True
                    entry["erratum_candidate_id"] = erratum["erratum_id"] if erratum else None
                    entry["erratum_status"] = erratum["status"] if erratum else None
                else:
                    replacements = self.store.state.get("replacements", {})
                    repl = next(
                        (
                            r
                            for r in replacements.values()
                            if r["analysis_id"] == aid and metric_id in r["affected_metric_ids"]
                        ),
                        None,
                    )
                    entry["atomic_replaced"] = repl is not None
                    if repl:
                        entry["comparison_before"] = repl["comparison_before"]
                        entry["comparison_after"] = repl["comparison_after"]
                impacts.append(entry)

            return {
                "metric_id": metric_id,
                "visit_id": metric["visit_id"],
                "sequence": metric["sequence"],
                "name": metric["name"],
                "recomputed": current.get("recomputed", False),
                "status": status,
                "reason_unchanged": reason_unchanged,
                "current": copy.deepcopy(current),
                "calibration_used": calibration,
                "plans": plan_refs,
                "stage_comparison_impacts": impacts,
            }

    def get_analysis(self, analysis_id):
        with self._lock:
            analysis = self._get_analysis(analysis_id)
            result = copy.deepcopy(analysis)
            result["errata"] = [
                copy.deepcopy(e)
                for e in self.store.state["errata"].values()
                if e["analysis_id"] == analysis_id
            ]
            replacements = self.store.state.get("replacements", {})
            result["replacements_detail"] = [
                copy.deepcopy(replacements[rid])
                for rid in analysis["replacements"]
                if rid in replacements
            ]
            return result

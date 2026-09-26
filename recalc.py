"""扫描设备校准漂移的版本谱系与定向重算。

本模块只依赖标准库，承载以下领域规则：

* 设备与校准区间构成版本谱系，后到的区间会闭合前一区间；
* 校准通告按 (设备, 生效起点, 参数指纹) 去重，重复通告不重复排队；
* 同一设备区间收到矛盾参数时区间隔离，暂停后续计划直到裁决；
* 重算计划按采集时点只圈定受影响中心 / 序列 / 指标，并排除
  已撤回参与者与同意范围不足的访视；
* 计划须经质控（漂移证据）与统计负责人（分析影响）双重批准；
* 已冻结 / 已发表结果保留原快照，通过勘误候选连接新结果；
  未冻结结果原子替换；
* 执行以单元为粒度记录检查点，故障恢复后从最后完成单元继续；
* explain 接口说明指标是否重算、保持原值的原因、采用的校准版本
  及对人生阶段比较的影响。
"""

import copy
import hashlib
import json
import os
import threading
import uuid
from datetime import datetime

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

RESEARCH_SCOPE = "research"

CAL_DRAFT = "draft"
CAL_ACTIVE = "active"
CAL_SUPERSEDED = "superseded"
CAL_QUARANTINED = "quarantined"

NOTICE_QUEUED = "queued"
NOTICE_DEDUP = "deduplicated"
NOTICE_QUARANTINED = "quarantined"
NOTICE_APPLIED = "applied"

PLAN_PROPOSED = "proposed"
PLAN_APPROVED = "approved"
PLAN_RUNNING = "running"
PLAN_INTERRUPTED = "interrupted"
PLAN_COMPLETED = "completed"

UNIT_PENDING = "pending"
UNIT_DONE = "done"

METRIC_DRAFT = "draft"
METRIC_FROZEN = "frozen"
METRIC_PUBLISHED = "published"


class RecalcError(ValueError):
    """输入或状态不满足领域规则，HTTP 层映射为 400。"""


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _parse_ts(value):
    if not isinstance(value, str):
        raise RecalcError(f"时间点必须是 ISO 字符串: {value!r}")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise RecalcError(f"时间点格式无法解析: {value!r}") from exc


def params_fingerprint(params):
    """校准参数的规范化指纹，参数顺序不同也视为同一组参数。"""
    blob = json.dumps(params, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _new_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _calibrated_value(raw_signal, cal_params):
    gain = float(cal_params.get("gain", 1.0))
    bias = float(cal_params.get("bias", 0.0))
    return round(float(raw_signal) * gain + bias, 6)


def _find(seq, **criteria):
    for item in seq:
        if all(item.get(key) == value for key, value in criteria.items()):
            return item
    return None


def _require(container, error):
    if container is None:
        raise RecalcError(error)
    return container


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------

class RecalcStore:
    """线程安全的 JSON 快照存储。

    生产环境可替换为数据库实现，领域函数只依赖本类暴露的集合与锁。
    """

    def __init__(self, path=None):
        self.path = path
        self.lock = threading.RLock()
        self.data = {
            "sites": [],
            "devices": [],
            "calibrations": [],
            "notices": [],
            "visits": [],
            "consents": [],
            "withdrawals": [],
            "metrics": [],
            "comparisons": [],
            "plans": [],
        }
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            for key in self.data:
                self.data[key] = loaded.get(key, [])

    def save(self):
        if not self.path:
            return
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(self.data, handle, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.data)


# ---------------------------------------------------------------------------
# 基础登记：中心 / 设备 / 访视 / 同意 / 指标 / 阶段比较
# ---------------------------------------------------------------------------

def register_site(store, payload):
    site_id = payload.get("site_id") if isinstance(payload, dict) else payload
    if not site_id:
        raise RecalcError("缺少 site_id")
    with store.lock:
        if not _find(store.data["sites"], site_id=site_id):
            store.data["sites"].append({"site_id": site_id})
            store.save()
        return {"site_id": site_id}


def register_device(store, payload):
    device_id = payload.get("device_id")
    site_id = payload.get("site_id")
    if not device_id or not site_id:
        raise RecalcError("登记设备需要 device_id 与 site_id")
    with store.lock:
        _require(_find(store.data["sites"], site_id=site_id), f"中心不存在: {site_id}")
        existing = _find(store.data["devices"], device_id=device_id)
        if existing is None:
            store.data["devices"].append({"device_id": device_id, "site_id": site_id})
            store.save()
        elif existing["site_id"] != site_id:
            raise RecalcError(f"设备 {device_id} 已登记在其他中心")
        return {"device_id": device_id, "site_id": site_id}


def register_visit(store, payload):
    required = ("visit_id", "participant_id", "site_id", "device_id", "acquired_at")
    for key in required:
        if payload.get(key) in (None, ""):
            raise RecalcError(f"访视缺少字段: {key}")
    acquired_at = _parse_ts(payload["acquired_at"])
    with store.lock:
        _require(_find(store.data["sites"], site_id=payload["site_id"]), "中心不存在")
        device = _require(
            _find(store.data["devices"], device_id=payload["device_id"]),
            "设备不存在",
        )
        if device["site_id"] != payload["site_id"]:
            raise RecalcError("设备不属于登记中心，跨中心采集不允许")
        if _find(store.data["visits"], visit_id=payload["visit_id"]):
            raise RecalcError(f"访视已存在: {payload['visit_id']}")
        sequences = []
        for seq in payload.get("sequences", []):
            name = seq.get("name")
            if not name:
                raise RecalcError("序列缺少 name")
            sequences.append({"name": name, "cal_version": seq.get("cal_version")})
        visit = {
            "visit_id": payload["visit_id"],
            "participant_id": payload["participant_id"],
            "site_id": payload["site_id"],
            "device_id": payload["device_id"],
            "acquired_at": payload["acquired_at"],
            "acquired_ts": acquired_at.isoformat(),
            "sequences": sequences,
        }
        store.data["visits"].append(visit)
        store.save()
        return copy.deepcopy(visit)


def register_consent(store, payload):
    participant_id = payload.get("participant_id")
    scope = payload.get("scope", RESEARCH_SCOPE)
    valid_from = _parse_ts(payload.get("valid_from"))
    valid_to = _parse_ts(payload["valid_to"]) if payload.get("valid_to") else None
    if valid_to and valid_to <= valid_from:
        raise RecalcError("同意失效时间必须晚于生效时间")
    with store.lock:
        record = {
            "participant_id": participant_id,
            "scope": scope,
            "valid_from": payload["valid_from"],
            "valid_to": payload.get("valid_to"),
        }
        store.data["consents"].append(record)
        store.save()
        return copy.deepcopy(record)


def register_withdrawal(store, payload):
    participant_id = payload.get("participant_id")
    withdrawn_at = _parse_ts(payload.get("withdrawn_at"))
    with store.lock:
        record = {"participant_id": participant_id, "withdrawn_at": payload["withdrawn_at"]}
        store.data["withdrawals"].append(record)
        store.save()
        return copy.deepcopy(record)


def register_metric(store, payload):
    required = ("metric_id", "participant_id", "visit_id", "sequence", "name")
    for key in required:
        if payload.get(key) in (None, ""):
            raise RecalcError(f"指标缺少字段: {key}")
    status = payload.get("status", METRIC_DRAFT)
    if status not in (METRIC_DRAFT, METRIC_FROZEN, METRIC_PUBLISHED):
        raise RecalcError(f"未知指标状态: {status}")
    with store.lock:
        visit = _require(
            _find(store.data["visits"], visit_id=payload["visit_id"]),
            f"访视不存在: {payload['visit_id']}",
        )
        if _find(store.data["metrics"], metric_id=payload["metric_id"]):
            raise RecalcError(f"指标已存在: {payload['metric_id']}")
        derived_from = list(payload.get("derived_from", []))
        metric = {
            "metric_id": payload["metric_id"],
            "participant_id": payload["participant_id"],
            "visit_id": payload["visit_id"],
            "site_id": visit["site_id"],
            "sequence": payload["sequence"],
            "name": payload["name"],
            "status": status,
            "cal_version": payload.get("cal_version"),
            "derived_from": derived_from,
            "formula": payload.get("formula", "mean" if derived_from else None),
            # 原始信号值；派生指标可为空，取值由 formula 决定。
            "raw_signal": payload.get("raw_signal", payload.get("value")),
            "value": payload.get("value"),
            "replaced_by_unit": None,
            "errata_candidate": None,
        }
        if not derived_from and metric["raw_signal"] is not None:
            metric["value"] = metric["raw_signal"]
        store.data["metrics"].append(metric)
        store.save()
        return copy.deepcopy(metric)


def register_comparison(store, payload):
    required = ("comparison_id", "life_stage_pair", "metric_ids")
    for key in required:
        if not payload.get(key):
            raise RecalcError(f"阶段比较缺少字段: {key}")
    status = payload.get("status", METRIC_DRAFT)
    if status not in (METRIC_DRAFT, METRIC_FROZEN, METRIC_PUBLISHED):
        raise RecalcError(f"未知阶段比较状态: {status}")
    with store.lock:
        for metric_id in payload["metric_ids"]:
            _require(
                _find(store.data["metrics"], metric_id=metric_id),
                f"指标不存在: {metric_id}",
            )
        comparison = {
            "comparison_id": payload["comparison_id"],
            "life_stage_pair": list(payload["life_stage_pair"]),
            "metric_ids": list(payload["metric_ids"]),
            "status": status,
            "value": payload.get("value"),
            "replaced_by_plan": None,
            "errata_candidate": None,
        }
        store.data["comparisons"].append(comparison)
        store.save()
        return copy.deepcopy(comparison)


# ---------------------------------------------------------------------------
# 校准谱系与通告
# ---------------------------------------------------------------------------

def _interval_active_at(cal, ts):
    start = datetime.fromisoformat(cal["valid_from"])
    if ts < start:
        return False
    if cal.get("valid_to"):
        return ts < datetime.fromisoformat(cal["valid_to"])
    return True


def _device_calibrations(store, device_id):
    return [c for c in store.data["calibrations"] if c["device_id"] == device_id]


def _open_interval(store, device_id):
    for cal in _device_calibrations(store, device_id):
        if cal["status"] == CAL_ACTIVE and not cal.get("valid_to"):
            return cal
    return None


def _quarantine_lock(store, device_id, valid_from):
    """该区间是否存在未裁决的矛盾参数。"""
    for notice in store.data["notices"]:
        if (
            notice["device_id"] == device_id
            and notice["valid_from"] == valid_from
            and notice["status"] == NOTICE_QUARANTINED
        ):
            return notice
    return None


def device_lineage(store, device_id):
    with store.lock:
        _require(_find(store.data["devices"], device_id=device_id), f"设备不存在: {device_id}")
        calibrations = sorted(
            (copy.deepcopy(c) for c in _device_calibrations(store, device_id)),
            key=lambda c: c["valid_from"],
        )
        notices = sorted(
            (
                copy.deepcopy(n)
                for n in store.data["notices"]
                if n["device_id"] == device_id
            ),
            key=lambda n: n["received_seq"],
        )
        return {"device_id": device_id, "calibrations": calibrations, "notices": notices}


def submit_calibration_notice(store, payload):
    """登记校准通告：去重、矛盾隔离或排队等待生成计划。"""
    device_id = payload.get("device_id")
    valid_from = payload.get("valid_from")
    params = payload.get("params")
    _parse_ts(valid_from)
    if not isinstance(params, dict):
        raise RecalcError("校准通告必须包含 params 校准参数对象")
    fingerprint = params_fingerprint(params)
    with store.lock:
        _require(_find(store.data["devices"], device_id=device_id), f"设备不存在: {device_id}")
        if _quarantine_lock(store, device_id, valid_from):
            raise RecalcError("该设备区间存在待裁决的矛盾参数，暂不接收新通告")

        # 同一区间、同一参数指纹：重复通告，绝不重复排队。
        prior = _find(
            store.data["notices"],
            device_id=device_id,
            valid_from=valid_from,
            params_hash=fingerprint,
        )
        notice_seq = 1 + len(store.data["notices"])
        if prior is not None:
            notice = {
                "notice_id": _new_id("notice"),
                "device_id": device_id,
                "valid_from": valid_from,
                "params": copy.deepcopy(params),
                "params_hash": fingerprint,
                "status": NOTICE_DEDUP,
                "duplicate_of": prior["notice_id"],
                "received_seq": notice_seq,
            }
            store.data["notices"].append(notice)
            store.save()
            return {
                "notice_id": notice["notice_id"],
                "status": NOTICE_DEDUP,
                "duplicate_of": prior["notice_id"],
            }

        # 同一区间但参数不同：矛盾，双方隔离并暂停。
        conflicting = _find(
            store.data["notices"],
            device_id=device_id,
            valid_from=valid_from,
        )
        if conflicting is not None and conflicting["status"] in (
            NOTICE_QUEUED,
            NOTICE_APPLIED,
        ):
            conflicting["status"] = NOTICE_QUARANTINED
            conflict_ids = [conflicting["notice_id"]]
            # 已据旧通告展开的校准版本一并隔离。
            if conflicting.get("cal_id"):
                old_cal = _find(
                    store.data["calibrations"], cal_id=conflicting["cal_id"]
                )
                if old_cal:
                    old_cal["status"] = CAL_QUARANTINED
            # 若矛盾通告已展开成计划，计划一并挂起。
            for plan in store.data["plans"]:
                if plan["trigger_notice_id"] == conflicting["notice_id"] and plan[
                    "status"
                ] in (PLAN_PROPOSED, PLAN_APPROVED):
                    plan["status"] = "suspended"
        else:
            conflict_ids = [conflicting["notice_id"]] if conflicting else []

        notice = {
            "notice_id": _new_id("notice"),
            "device_id": device_id,
            "valid_from": valid_from,
            "valid_to": payload.get("valid_to"),
            "params": copy.deepcopy(params),
            "params_hash": fingerprint,
            "received_seq": notice_seq,
            "reported_by": payload.get("reported_by"),
        }
        if conflict_ids:
            notice["status"] = NOTICE_QUARANTINED
            notice["conflict_with"] = conflict_ids
            store.data["notices"].append(notice)
            store.save()
            return {
                "notice_id": notice["notice_id"],
                "status": NOTICE_QUARANTINED,
                "conflict_with": conflict_ids,
            }

        notice["status"] = NOTICE_QUEUED
        store.data["notices"].append(notice)
        store.save()
        return {"notice_id": notice["notice_id"], "status": NOTICE_QUEUED}


def resolve_calibration_conflict(store, payload):
    """裁决设备区间的矛盾参数：胜出版本生效，败方隔离。"""
    device_id = payload["device_id"]
    valid_from = payload["valid_from"]
    winner_id = payload["winning_notice_id"]
    with store.lock:
        quarantined = [
            n
            for n in store.data["notices"]
            if n["device_id"] == device_id
            and n["valid_from"] == valid_from
            and n["status"] == NOTICE_QUARANTINED
        ]
        if not quarantined:
            raise RecalcError("该区间没有待裁决的矛盾参数")
        winner = _find(quarantined, notice_id=winner_id)
        _require(winner, "胜方通告不属于该矛盾区间")

        # 关闭此前开放区间（若胜方起点与之相同则不闭合，裁决替换的就是它）。
        open_cal = _open_interval(store, device_id)
        if open_cal and open_cal["valid_from"] != valid_from:
            open_cal["valid_to"] = valid_from
            open_cal["status"] = CAL_SUPERSEDED

        cal = _find(
            store.data["calibrations"],
            device_id=device_id,
            valid_from=valid_from,
            params_hash=winner["params_hash"],
        )
        if cal is None:
            cal = {
                "cal_id": _new_id("cal"),
                "device_id": device_id,
                "version": payload.get(
                    "version", f"v{1 + len(_device_calibrations(store, device_id))}"
                ),
                "params": copy.deepcopy(winner["params"]),
                "params_hash": winner["params_hash"],
                "valid_from": valid_from,
                "valid_to": winner.get("valid_to"),
                "status": CAL_ACTIVE,
                "source_notice_id": winner["notice_id"],
            }
            store.data["calibrations"].append(cal)
        else:
            cal["status"] = CAL_ACTIVE

        winner["status"] = NOTICE_APPLIED
        winner["cal_id"] = cal["cal_id"]
        loser_ids = []
        for notice in quarantined:
            if notice["notice_id"] != winner_id:
                notice["status"] = CAL_QUARANTINED
                loser_ids.append(notice["notice_id"])

        # 处理矛盾期间挂起的计划：
        # - 触发通告胜出：计划恢复并重指向胜方校准（审批保留）；
        # - 触发通告落败：旧计划作废，基于胜方通告生成新计划，
        #   因为审批是在另一组修正参数下给出的，须重新走双门禁。
        resumed_plan_ids = []
        cancelled_plan_ids = []
        for plan in store.data["plans"]:
            if plan["status"] != "suspended":
                continue
            if plan["trigger_notice_id"] == winner_id:
                plan["status"] = (
                    PLAN_APPROVED
                    if all(plan["approvals"].values())
                    else PLAN_PROPOSED
                )
                plan["cal_id_after"] = cal["cal_id"]
                plan["cal_version_after"] = cal["version"]
                resumed_plan_ids.append(plan["plan_id"])
            else:
                plan["status"] = "cancelled_conflict_lost"
                plan["superseded_by_notice"] = winner_id
                cancelled_plan_ids.append(plan["plan_id"])

        new_plan_id = None
        if cancelled_plan_ids and not _find(
            store.data["plans"], trigger_notice_id=winner_id
        ):
            new_plan = create_plan(store, {"notice_id": winner_id})
            new_plan_id = new_plan["plan_id"]

        store.save()
        return {
            "device_id": device_id,
            "valid_from": valid_from,
            "winning_notice_id": winner_id,
            "cal_id": cal["cal_id"],
            "loser_notice_ids": loser_ids,
            "resumed_plan_ids": resumed_plan_ids,
            "cancelled_plan_ids": cancelled_plan_ids,
            "new_plan_id": new_plan_id,
        }


# ---------------------------------------------------------------------------
# 同意与撤回判定
# ---------------------------------------------------------------------------

def _participant_eligible(store, participant_id, acquired_dt):
    if _find(store.data["withdrawals"], participant_id=participant_id):
        return False, "participant_withdrawn"
    for consent in store.data["consents"]:
        if consent["participant_id"] != participant_id or consent["scope"] != RESEARCH_SCOPE:
            continue
        start = datetime.fromisoformat(consent["valid_from"])
        end = (
            datetime.fromisoformat(consent["valid_to"])
            if consent.get("valid_to")
            else None
        )
        if acquired_dt >= start and (end is None or acquired_dt < end):
            return True, None
    return False, "consent_out_of_scope"


# ---------------------------------------------------------------------------
# 重算计划
# ---------------------------------------------------------------------------

def _corrected_cal_for_notice(store, notice):
    """根据通告落实修正后的校准区间，并闭合旧区间。"""
    device_id = notice["device_id"]
    valid_from = notice["valid_from"]
    existing = _find(
        store.data["calibrations"],
        device_id=device_id,
        valid_from=valid_from,
        params_hash=notice["params_hash"],
    )
    if existing:
        return existing
    open_cal = _open_interval(store, device_id)
    if open_cal and open_cal["valid_from"] != valid_from:
        open_cal["valid_to"] = valid_from
        open_cal["status"] = CAL_SUPERSEDED
    cal = {
        "cal_id": _new_id("cal"),
        "device_id": device_id,
        "version": f"v{1 + len(_device_calibrations(store, device_id))}",
        "params": copy.deepcopy(notice["params"]),
        "params_hash": notice["params_hash"],
        "valid_from": valid_from,
        "valid_to": notice.get("valid_to"),
        "status": CAL_ACTIVE,
        "source_notice_id": notice["notice_id"],
    }
    store.data["calibrations"].append(cal)
    notice["status"] = NOTICE_APPLIED
    notice["cal_id"] = cal["cal_id"]
    return cal


def _affected_visits(store, device_id, valid_from, valid_to):
    start = datetime.fromisoformat(valid_from)
    end = datetime.fromisoformat(valid_to) if valid_to else None
    affected = []
    for visit in store.data["visits"]:
        if visit["device_id"] != device_id:
            continue
        acquired = datetime.fromisoformat(visit["acquired_ts"])
        if acquired < start:
            continue
        if end and acquired >= end:
            continue
        affected.append(visit)
    return affected


def _sequence_set(visits):
    names = set()
    for visit in visits:
        for seq in visit["sequences"]:
            names.add(seq["name"])
    return names


def create_plan(store, payload):
    notice_id = payload.get("notice_id")
    with store.lock:
        notice = _require(
            _find(store.data["notices"], notice_id=notice_id),
            f"通告不存在: {notice_id}",
        )
        if notice["status"] == NOTICE_DEDUP:
            raise RecalcError("重复通告不生成重算计划")
        if notice["status"] == NOTICE_QUARANTINED:
            raise RecalcError("矛盾参数待裁决，暂不能生成计划")
        if _find(store.data["plans"], trigger_notice_id=notice_id):
            raise RecalcError("该通告已存在重算计划，禁止重复排队")

        corrected_cal = _corrected_cal_for_notice(store, notice)
        valid_to = notice.get("valid_to") or corrected_cal.get("valid_to")
        visits = _affected_visits(store, notice["device_id"], notice["valid_from"], valid_to)

        device = _find(store.data["devices"], device_id=notice["device_id"])
        all_sequences = _sequence_set(visits)
        seq_filter = set(payload.get("sequences") or [])
        sequences = sorted(seq_filter & all_sequences) if seq_filter else sorted(all_sequences)
        metric_filter = set(payload.get("metric_names") or [])

        included_visits = set()
        excluded_visits = []
        excluded_visit_ids = set()

        def note_excluded(visit_id, participant_id, reason):
            if visit_id in excluded_visit_ids:
                return
            excluded_visit_ids.add(visit_id)
            excluded_visits.append(
                {"visit_id": visit_id, "participant_id": participant_id, "reason": reason}
            )

        for visit in visits:
            eligible, reason = _participant_eligible(
                store, visit["participant_id"], datetime.fromisoformat(visit["acquired_ts"])
            )
            if eligible:
                included_visits.add(visit["visit_id"])
            else:
                note_excluded(visit["visit_id"], visit["participant_id"], reason)

        # 直接受影响叶子指标按访视同意状态分为纳入与阻断。
        interval_visit_ids = {v["visit_id"] for v in visits}
        leaf_candidates = [
            m
            for m in store.data["metrics"]
            if m["visit_id"] in interval_visit_ids
            and m["sequence"] in sequences
            and (not metric_filter or m["name"] in metric_filter)
            and not m["derived_from"]
        ]
        affected_ids = set()
        blocked = {}  # metric_id -> 阻断原因
        for metric in leaf_candidates:
            if metric["visit_id"] in included_visits:
                affected_ids.add(metric["metric_id"])
            else:
                reason = next(
                    e["reason"]
                    for e in excluded_visits
                    if e["visit_id"] == metric["visit_id"]
                )
                blocked[metric["metric_id"]] = reason

        # 派生闭包固定点：上游有阻断则一并挂起；上游受影响则纳入，
        # 同时对其自身访视做同意/撤回校验。
        changed = True
        while changed:
            changed = False
            for metric in store.data["metrics"]:
                mid = metric["metric_id"]
                if mid in affected_ids or mid in blocked or not metric["derived_from"]:
                    continue
                refs = metric["derived_from"]
                if any(ref in blocked for ref in refs):
                    blocked[mid] = "upstream_source_excluded"
                    changed = True
                    continue
                if any(ref in affected_ids for ref in refs):
                    visit = _find(store.data["visits"], visit_id=metric["visit_id"])
                    eligible, reason = _participant_eligible(
                        store, visit["participant_id"],
                        datetime.fromisoformat(visit["acquired_ts"]),
                    )
                    if eligible:
                        affected_ids.add(mid)
                        included_visits.add(metric["visit_id"])
                    else:
                        note_excluded(metric["visit_id"], visit["participant_id"], reason)
                        blocked[mid] = reason
                    changed = True
        recomputed = [
            _find(store.data["metrics"], metric_id=mid) for mid in sorted(affected_ids)
        ]

        # 以访视为单元组织重算。
        by_visit = {}
        for metric in recomputed:
            by_visit.setdefault(metric["visit_id"], []).append(metric["metric_id"])

        units = []
        for visit_id in sorted(by_visit):
            visit = _find(store.data["visits"], visit_id=visit_id)
            units.append(
                {
                    "unit_id": f"unit-{notice_id}-{visit_id}",
                    "visit_id": visit_id,
                    "participant_id": visit["participant_id"],
                    "site_id": visit["site_id"],
                    "in_calibration_interval": visit_id in interval_visit_ids,
                    "metric_ids": sorted(by_visit[visit_id]),
                    "status": UNIT_PENDING,
                }
            )

        plan = {
            "plan_id": _new_id("plan"),
            "trigger_notice_id": notice_id,
            "device_id": notice["device_id"],
            "site_id": device["site_id"],
            "interval": {"start": notice["valid_from"], "end": valid_to},
            "cal_id_before": None,
            "cal_id_after": corrected_cal["cal_id"],
            "cal_version_after": corrected_cal["version"],
            "sequences": sequences,
            "metric_names": sorted(metric_filter) if metric_filter else [],
            "status": PLAN_PROPOSED,
            "approvals": {"qc": None, "statistician": None},
            "units": units,
            "excluded_visits": excluded_visits,
            "blocked_metrics": [
                {"metric_id": mid, "reason": reason}
                for mid, reason in sorted(blocked.items())
            ],
            "completed_count": 0,
            "comparison_impacts": [],
        }
        prior_active = [
            c
            for c in _device_calibrations(store, notice["device_id"])
            if c["status"] == CAL_SUPERSEDED and c.get("valid_to") == notice["valid_from"]
        ]
        if prior_active:
            plan["cal_id_before"] = prior_active[0]["cal_id"]
        store.data["plans"].append(plan)
        store.save()
        return _plan_view(store, plan)


def _plan_view(store, plan):
    view = copy.deepcopy(plan)
    view["unit_count"] = len(plan["units"])
    return view


def get_plan(store, plan_id):
    with store.lock:
        plan = _require(_find(store.data["plans"], plan_id=plan_id), f"计划不存在: {plan_id}")
        return _plan_view(store, plan)


def approve_plan(store, plan_id, role, approver, evidence=None):
    if role not in ("qc", "statistician"):
        raise RecalcError("审批角色必须是 qc 或 statistician")
    with store.lock:
        plan = _require(_find(store.data["plans"], plan_id=plan_id), f"计划不存在: {plan_id}")
        if plan["status"] not in (PLAN_PROPOSED, PLAN_APPROVED, "suspended"):
            raise RecalcError(f"当前计划状态 {plan['status']} 不可审批")
        if plan["status"] == "suspended":
            raise RecalcError("计划因矛盾参数已挂起，等待裁决")
        if role == "statistician" and plan["approvals"]["qc"] is None:
            raise RecalcError("须先由质控负责人确认漂移证据")
        plan["approvals"][role] = {"approver": approver, "evidence": evidence}
        if all(plan["approvals"].values()):
            plan["status"] = PLAN_APPROVED
        store.save()
        return _plan_view(store, plan)


# ---------------------------------------------------------------------------
# 计划执行：检查点、冻结快照、勘误候选、阶段比较影响
# ---------------------------------------------------------------------------

def _source_value(metric):
    """派生重算时源指标的取值：已有勘误候选则采用候选新值。"""
    if metric.get("errata_candidate"):
        return metric["errata_candidate"]["new_value"]
    return metric["value"]


def _recompute_metric(store, metric, corrected_cal):
    if not metric["derived_from"]:
        return _calibrated_value(metric["raw_signal"], corrected_cal["params"])
    inputs = [
        _find(store.data["metrics"], metric_id=ref) for ref in metric["derived_from"]
    ]
    values = [_source_value(m) for m in inputs if m and _source_value(m) is not None]
    if metric.get("formula", "mean") == "sum":
        return round(sum(values), 6)
    return round(sum(values) / len(values), 6) if values else None


def _metric_outputs(store, plan, metric, new_value, unit_id):
    """按冻结状态产出原子替换或勘误候选。"""
    corrected_cal = _find(store.data["calibrations"], cal_id=plan["cal_id_after"])
    before_cal = (
        _find(store.data["calibrations"], cal_id=plan["cal_id_before"])
        if plan.get("cal_id_before")
        else None
    )
    old_cal_version = metric.get("cal_version") or (
        before_cal["version"] if before_cal else None
    )
    outcome = {
        "metric_id": metric["metric_id"],
        "old_value": metric["value"],
        "new_value": new_value,
        "cal_version_after": corrected_cal["version"],
    }
    if metric["status"] == METRIC_DRAFT:
        metric["value"] = new_value
        metric["cal_version"] = corrected_cal["version"]
        metric["replaced_by_unit"] = unit_id
        outcome["action"] = "atomic_replaced"
    else:
        # 已冻结 / 已发表：原值与快照保持不动，仅挂接勘误候选。
        candidate_id = _new_id("errata")
        metric["errata_candidate"] = {
            "candidate_id": candidate_id,
            "plan_id": plan["plan_id"],
            "unit_id": unit_id,
            "old_snapshot": {
                "value": metric["value"],
                "cal_version": old_cal_version,
                "status": metric["status"],
            },
            "new_value": new_value,
            "new_cal_version": corrected_cal["version"],
            "status": "candidate",
        }
        outcome["action"] = "errata_candidate"
        outcome["errata_candidate_id"] = candidate_id
    return outcome


def run_plan(store, plan_id, fail_after=None):
    """执行计划。fail_after 用于故障演练：完成 N 个单元后中断。"""
    with store.lock:
        plan = _require(_find(store.data["plans"], plan_id=plan_id), f"计划不存在: {plan_id}")
        if plan["status"] == "suspended":
            raise RecalcError("计划因矛盾参数挂起，等待裁决")
        if plan["status"] not in (PLAN_APPROVED, PLAN_INTERRUPTED):
            raise RecalcError(f"计划状态 {plan['status']} 不可执行，需双重批准")

        corrected_cal = _find(store.data["calibrations"], cal_id=plan["cal_id_after"])
        plan["status"] = PLAN_RUNNING
        done_since_resume = 0
        results = []
        interrupted = False

        def visit_ts(unit):
            visit = _find(store.data["visits"], visit_id=unit["visit_id"])
            return visit["acquired_ts"]

        # 按访视采集时间排序单元，使跨访视派生依赖先算上游。
        ordered = sorted(plan["units"], key=visit_ts)
        for unit in ordered:
            if unit["status"] == UNIT_DONE:
                continue
            visit = _find(store.data["visits"], visit_id=unit["visit_id"])
            eligible, reason = _participant_eligible(
                store, unit["participant_id"], datetime.fromisoformat(visit["acquired_ts"])
            )
            if not eligible:
                # 执行前再次校验：撤回 / 收窄同意的数据不得借重算回流。
                unit["status"] = UNIT_DONE
                unit["skipped"] = reason
                if not any(
                    e["visit_id"] == unit["visit_id"] for e in plan["excluded_visits"]
                ):
                    plan["excluded_visits"].append(
                        {"visit_id": unit["visit_id"], "participant_id": unit["participant_id"], "reason": reason}
                    )
                # 运行时阻断同样登记，使引用其指标的比较被挂起、可被 explain 解释。
                known_blocked = {b["metric_id"] for b in plan.setdefault("blocked_metrics", [])}
                for mid in unit["metric_ids"]:
                    if mid not in known_blocked:
                        plan["blocked_metrics"].append({"metric_id": mid, "reason": reason})
                        known_blocked.add(mid)
            else:
                # 叶子指标先算，派生指标按依赖闭包拓扑排序，
                # 以支持跨访视 / 跨单元的派生链。
                unit_metrics = [
                    _find(store.data["metrics"], metric_id=mid) for mid in unit["metric_ids"]
                ]
                by_id = {m["metric_id"]: m for m in unit_metrics}
                ordered_metrics = []
                placed = set()

                def place(metric):
                    if metric["metric_id"] in placed:
                        return
                    for ref in metric["derived_from"]:
                        if ref in by_id:
                            place(by_id[ref])
                    placed.add(metric["metric_id"])
                    ordered_metrics.append(metric)

                for metric in unit_metrics:
                    place(metric)

                unit["outcomes"] = []
                for metric in ordered_metrics:
                    new_value = _recompute_metric(store, metric, corrected_cal)
                    outcome = _metric_outputs(store, plan, metric, new_value, unit["unit_id"])
                    unit["outcomes"].append(outcome)
                    results.append(outcome)
                unit["status"] = UNIT_DONE

            plan["completed_count"] = sum(1 for u in plan["units"] if u["status"] == UNIT_DONE)
            done_since_resume += 1
            store.save()  # 每个单元完成即落盘检查点

            if fail_after is not None and done_since_resume >= fail_after:
                plan["status"] = PLAN_INTERRUPTED
                store.save()
                interrupted = True
                break

        if not interrupted:
            plan["comparison_impacts"] = _refresh_comparisons(store, plan)
            plan["status"] = PLAN_COMPLETED
            store.save()
        return _plan_view(store, plan)


def _refresh_comparisons(store, plan):
    """汇总受影响的人生阶段比较，按冻结状态决定替换或勘误。"""
    impacted_metric_ids = set()
    for unit in plan["units"]:
        if unit.get("skipped"):
            continue  # 运行时被同意/撤回门槛跳过的单元不算已影响
        impacted_metric_ids.update(unit.get("metric_ids", []))

    impacts = []
    blocked_ids = {entry["metric_id"] for entry in plan.get("blocked_metrics", [])}
    for comparison in store.data["comparisons"]:
        members = set(comparison["metric_ids"])
        hit = members & impacted_metric_ids
        blocked_members = members & blocked_ids
        if not hit and not blocked_members:
            continue
        impact = {
            "comparison_id": comparison["comparison_id"],
            "affected_metric_ids": sorted(hit),
            "old_value": comparison["value"],
        }
        if blocked_members:
            # 比较同时引用重算指标与被阻断指标时不得静默混用新旧值：
            # 比较整体挂起，保留原值，等待阻断解除（恢复同意/撤回裁决）。
            impact["new_value"] = None
            impact["blocked_metric_ids"] = sorted(blocked_members)
            impact["action"] = "held_blocked_source"
            impacts.append(impact)
            continue
        new_value = _recompute_comparison(store, comparison)
        impact["new_value"] = new_value
        if comparison["status"] == METRIC_DRAFT:
            comparison["value"] = new_value
            comparison["replaced_by_plan"] = plan["plan_id"]
            impact["action"] = "atomic_replaced"
        else:
            candidate_id = _new_id("errata-cmp")
            comparison["errata_candidate"] = {
                "candidate_id": candidate_id,
                "plan_id": plan["plan_id"],
                "old_snapshot": {
                    "value": comparison["value"],
                    "status": comparison["status"],
                },
                "new_value": new_value,
                "status": "candidate",
            }
            impact["action"] = "errata_candidate"
            impact["errata_candidate_id"] = candidate_id
        impacts.append(impact)
    return impacts


def _recompute_comparison(store, comparison):
    values = []
    for metric_id in comparison["metric_ids"]:
        metric = _find(store.data["metrics"], metric_id=metric_id)
        if metric:
            value = _source_value(metric)
            if value is not None:
                values.append(value)
    return round(sum(values) / len(values), 6) if values else None


# ---------------------------------------------------------------------------
# 指标可追溯解释
# ---------------------------------------------------------------------------

def explain_metric(store, metric_id):
    with store.lock:
        metric = _require(
            _find(store.data["metrics"], metric_id=metric_id), f"指标不存在: {metric_id}"
        )
        visit = _find(store.data["visits"], visit_id=metric["visit_id"])
        acquired_dt = datetime.fromisoformat(visit["acquired_ts"])

        # 该采集时点在当前谱系中对应的校准版本（修正后的版本）。
        lineage_cal = None
        for cal in _device_calibrations(store, visit["device_id"]):
            if _interval_active_at(cal, acquired_dt):
                lineage_cal = cal

        # 找到覆盖该指标的最近计划（若有多个，取最新）。
        covering = []
        blocked_in = []
        for plan in store.data["plans"]:
            for unit in plan["units"]:
                if metric_id in unit.get("metric_ids", []):
                    covering.append((plan, unit))
            for entry in plan.get("blocked_metrics", []):
                if entry["metric_id"] == metric_id:
                    blocked_in.append((plan, entry["reason"]))
        eligible, eligibility_reason = _participant_eligible(
            store, visit["participant_id"], acquired_dt
        )

        def resolve_current_version():
            """当前存储值实际由哪版校准产生。"""
            if metric.get("errata_candidate"):
                return metric["errata_candidate"]["old_snapshot"].get("cal_version")
            if metric.get("replaced_by_unit"):
                return metric.get("cal_version")
            # 保留原值的情形：优先取覆盖计划修正前的校准版本。
            for src_plan, _ in reversed(covering + blocked_in):
                before_id = src_plan.get("cal_id_before")
                if before_id:
                    before = _find(store.data["calibrations"], cal_id=before_id)
                    if before:
                        return before["version"]
            return lineage_cal["version"] if lineage_cal else None

        explanation = {
            "metric_id": metric_id,
            "visit_id": visit["visit_id"],
            "site_id": visit["site_id"],
            "acquired_at": visit["acquired_at"],
            "current_value": metric["value"],
            "current_cal_version": resolve_current_version(),
            "recomputed": False,
            "reason": None,
            "errata_candidate": None,
            "comparison_impacts": [],
        }

        # 与该指标相关的最新计划（覆盖执行或阻断），用于阶段比较影响。
        impact_plan = covering[-1][0] if covering else (
            blocked_in[-1][0] if blocked_in else None
        )

        if not eligible:
            explanation["reason"] = {
                "participant_withdrawn": "参与者已撤回，数据不得因重算重新进入",
                "consent_out_of_scope": "采集时点同意范围不足，重算排除该访视",
            }[eligibility_reason]
            explanation["exclusion_code"] = eligibility_reason
        elif not covering and blocked_in:
            plan, reason = blocked_in[-1]
            explanation["plan_id"] = plan["plan_id"]
            explanation["exclusion_code"] = reason
            explanation["reason"] = {
                "participant_withdrawn": "参与者已撤回，该指标不进入重算，原值保留",
                "consent_out_of_scope": "采集时点同意范围不足，该指标不进入重算，原值保留",
                "upstream_source_excluded": "其派生来源因撤回或同意不足被排除，下游一并挂起，原值保留",
            }.get(reason, f"该指标被阻断：{reason}")
        elif not covering:
            explanation["reason"] = (
                "采集时点不落在任何错误校准区间，或其中心/序列不在重算范围；原值保留"
            )
            explanation["exclusion_code"] = "out_of_affected_scope"
        else:
            plan, unit = covering[-1]
            explanation["plan_id"] = plan["plan_id"]
            explanation["plan_status"] = plan["status"]
            explanation["cal_version_after"] = plan["cal_version_after"]

            if unit.get("skipped"):
                explanation["reason"] = "执行时同意/撤回校验未通过，单元跳过"
                explanation["exclusion_code"] = unit["skipped"]
            elif plan["status"] != PLAN_COMPLETED and metric.get("replaced_by_unit") is None \
                    and metric.get("errata_candidate") is None:
                explanation["reason"] = f"重算计划尚未完成（{plan['status']}），原值暂时保留"
                explanation["exclusion_code"] = "plan_incomplete"
            else:
                outcome = next(
                    (o for o in unit.get("outcomes", []) if o["metric_id"] == metric_id), None
                )
                if metric.get("replaced_by_unit") == unit["unit_id"] or (
                    outcome and outcome["action"] == "atomic_replaced"
                ):
                    explanation["recomputed"] = True
                    explanation["reason"] = "未冻结分析，受影响输入已原子替换"
                    explanation["old_value"] = outcome["old_value"] if outcome else None
                    explanation["new_value"] = metric["value"]
                elif metric.get("errata_candidate"):
                    candidate = metric["errata_candidate"]
                    explanation["recomputed"] = True
                    explanation["reason"] = (
                        "分析已冻结/发表，原快照保留，新结果以勘误候选连接"
                    )
                    explanation["old_value"] = candidate["old_snapshot"]["value"]
                    explanation["new_value"] = candidate["new_value"]
                    explanation["errata_candidate"] = copy.deepcopy(candidate)

        # 阶段比较影响：覆盖/阻断计划中已登记的影响；其余比较标注待定。
        if impact_plan is not None:
            for comparison in store.data["comparisons"]:
                if metric_id not in comparison["metric_ids"]:
                    continue
                impact = next(
                    (
                        impact
                        for impact in impact_plan.get("comparison_impacts", [])
                        if impact["comparison_id"] == comparison["comparison_id"]
                    ),
                    None,
                )
                entry = {
                    "comparison_id": comparison["comparison_id"],
                    "life_stage_pair": comparison["life_stage_pair"],
                    "comparison_status": comparison["status"],
                }
                if impact:
                    entry.update(impact)
                elif impact_plan["status"] == PLAN_COMPLETED:
                    entry["action"] = "unaffected"
                else:
                    entry["action"] = "pending_plan_completion"
                explanation["comparison_impacts"].append(entry)

        return explanation

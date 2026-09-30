"""片区样本、实验室排队与恢复审批的合并记账状态机。

账本状态保存在事件 payload 的 ``sampling`` 字段中，所有函数都是纯函数：
输入当前 item 与登记数据，输出新状态与事件载荷，由 repository 在一个事务里落库。

样本生命周期::

    queued -> processing -> valid | invalid

- 实验室容量有限：满员时新样本排队 queued；样本出结果后释放实验位，队首 FIFO 晋升。
- 补交结果是否推翻原结论，按采样时刻 sampled_at 判定：采样更新者代表当前结论；
  更早的漏送结果补交后只留档（supersedes 指向更新样本），不改变当前结论。
- 完成时间晚于有效期的结果记为 invalid（过期结果不能作为达标依据）。
- 恢复前区域范围一变，当前轮全部结果作废（round 递增），必须重新取样。
"""

from .domain import ConflictError, DomainError, NotFoundError, parse_dt

# 角色
REGISTER_ROLES = {"lab", "field_operator", "analyst", "dispatcher"}
RESULT_ROLES = {"lab"}
SCOPE_ROLES = {"coordinator", "dispatcher"}
APPROVE_ROLES = {"coordinator", "regulator"}
PENDING_VIEW_ROLES = {"dispatcher", "regulator", "coordinator"}

DEFAULT_LAB_CAPACITY = 2


def _ensure_ledger(current, lab_capacity):
    sampling = current.get("sampling")
    if not sampling:
        sampling = {
            "round": 1,
            "lab_capacity": lab_capacity,
            "zones": {},          # zone_id -> 当前轮该片区的样本 sample_id 列表
            "samples": {},        # sample_id -> 样本记录
            "queue": [],          # 排队中的 sample_id（FIFO）
            "approvals": [],      # 审批记录（含每轮快照，旧记录留档）
        }
        current["sampling"] = sampling
    return sampling


def _processing_ids(sampling):
    return [sid for sid, rec in sampling["samples"].items()
            if rec["status"] == "processing" and rec["round"] == sampling["round"]]


def _promote_from_queue(sampling):
    """容量空闲时把队首样本晋升为 processing，返回本次晋升的 sample_id 列表。"""
    promoted = []
    capacity = int(sampling.get("lab_capacity", DEFAULT_LAB_CAPACITY))
    while len(_processing_ids(sampling)) < capacity and sampling["queue"]:
        sid = sampling["queue"].pop(0)
        rec = sampling["samples"].get(sid)
        if rec is None or rec["round"] != sampling["round"] or rec["status"] != "queued":
            continue
        rec["status"] = "processing"
        promoted.append(sid)
    return promoted


def register_sample(current, data, lab_capacity=DEFAULT_LAB_CAPACITY):
    """登记一片区样本。重复提交（同一 sample_id）幂等，不增加样本数。"""
    sampling = _ensure_ledger(current, lab_capacity)
    sample_id = data["sample_id"]
    zone_id = data["zone_id"]
    existing = sampling["samples"].get(sample_id)

    # 幂等：同一 sample_id 已存在则原样返回，绝不重复计数。
    if existing is not None:
        if existing["round"] != sampling["round"]:
            raise ConflictError("sample_voided", "该样本所属轮次已作废，请重新取样")
        if existing["zone_id"] != zone_id:
            raise ConflictError("sample_zone_mismatch", "同一编号样本不能改挂到其他片区")
        return existing, {"sample_id": sample_id, "dedup": True, "status": existing["status"]}

    sampled_dt = parse_dt(data["sampled_at"])
    zone_samples = sampling["zones"].setdefault(zone_id, [])
    for sid in zone_samples:
        other = sampling["samples"][sid]
        if parse_dt(other["sampled_at"]) == sampled_dt:
            raise ConflictError("duplicate_sample_time", "同一片区同一采样时刻的样本已存在")

    record = {
        "sample_id": sample_id,
        "zone_id": zone_id,
        "round": sampling["round"],
        "sampled_at": data["sampled_at"],
        "valid_until": data["valid_until"],
        "completed_at": None,
        "concentration": None,
        "status": "queued",
        "registered_by": data.get("actor"),
        "current": False,    # 是否为该片区当前结论样本
        "supersedes": None,  # 被哪个更新样本按采样时刻推翻
        "note": data.get("note", ""),
    }
    sampling["samples"][sample_id] = record
    zone_samples.append(sample_id)

    capacity = int(sampling.get("lab_capacity", DEFAULT_LAB_CAPACITY))
    if len(_processing_ids(sampling)) < capacity:
        record["status"] = "processing"
        entered = True
    else:
        sampling["queue"].append(sample_id)
        entered = False

    event = {
        "sample_id": sample_id,
        "zone_id": zone_id,
        "round": sampling["round"],
        "sampled_at": record["sampled_at"],
        "valid_until": record["valid_until"],
        "status": record["status"],
        "queued": not entered,
        "queue_length": len(sampling["queue"]),
    }
    return record, event


def _reconcile_zone(sampling, zone_id):
    """按采样时刻重排片区结论：最新采样的有效样本为 current，其余有效样本留档。"""
    records = [sampling["samples"][sid] for sid in sampling["zones"].get(zone_id, [])]
    for rec in records:
        rec["current"] = False
        rec["supersedes"] = None
    valid = sorted((r for r in records if r["status"] == "valid"),
                   key=lambda r: parse_dt(r["sampled_at"]), reverse=True)
    if not valid:
        return None
    newest = valid[0]
    newest["current"] = True
    for older in valid[1:]:
        older["supersedes"] = newest["sample_id"]
    return newest


def submit_result(current, data, limit):
    """实验室补交/完成一份样本结果，按采样时刻决定是否推翻原结论。"""
    sampling = current.get("sampling")
    if not sampling:
        raise NotFoundError("sample_not_found", "样本不存在，请先登记")
    sample_id = data["sample_id"]
    record = sampling["samples"].get(sample_id)
    if record is None:
        raise NotFoundError("sample_not_found", "样本不存在，请先登记")
    if record["round"] != sampling["round"]:
        raise ConflictError("sample_voided", "该样本所属轮次已作废，结果不能入账")
    if record["status"] in ("valid", "invalid"):
        raise ConflictError("result_already_submitted", "该样本结果已提交，不能重复补交")
    if record["status"] == "queued":
        position = sampling["queue"].index(sample_id) + 1 if sample_id in sampling["queue"] else None
        raise DomainError("sample_still_queued", "该样本仍在排队（前方 %s 份），暂不能提交结果" % (position - 1 if position else 0), 409)

    completed_dt = parse_dt(data["completed_at"])
    if completed_dt < parse_dt(record["sampled_at"]):
        raise DomainError("invalid_completion", "完成时间不能早于采样时刻")

    record["completed_at"] = data["completed_at"]
    record["concentration"] = data["concentration"]

    expired = completed_dt > parse_dt(record["valid_until"])
    if expired:
        record["status"] = "invalid"
        record["invalid_reason"] = "expired"
    else:
        record["status"] = "valid"
        record.pop("invalid_reason", None)

    over_limit = record["concentration"] > limit
    previous_current = None
    overturned = False
    if record["status"] == "valid":
        zone_records = [sampling["samples"][sid] for sid in sampling["zones"][record["zone_id"]]]
        previous_current = next((r["sample_id"] for r in zone_records if r["current"]), None)
        newest = _reconcile_zone(sampling, record["zone_id"])
        overturned = newest is not None and newest["sample_id"] == sample_id and previous_current not in (None, sample_id)

    promoted = _promote_from_queue(sampling)

    event = {
        "sample_id": sample_id,
        "zone_id": record["zone_id"],
        "round": sampling["round"],
        "completed_at": record["completed_at"],
        "concentration": record["concentration"],
        "limit": limit,
        "status": record["status"],
        "expired": expired,
        "over_limit": over_limit,
        "overturned_previous": previous_current if overturned else None,
        "promoted": promoted,
        "queue_length": len(sampling["queue"]),
    }
    return record, event


def change_scope(current, data):
    """恢复前区域范围变更：当前轮所有结果作废，轮次 +1，清空队列，重新取样。"""
    sampling = current.get("sampling")
    if not sampling:
        raise NotFoundError("ledger_not_found", "尚未开始任何采样登记")
    old_round = sampling["round"]
    new_round = old_round + 1
    affected = []
    for rec in sampling["samples"].values():
        if rec["round"] == old_round:
            rec["voided"] = True
            rec["void_reason"] = "scope_changed"
            affected.append(rec["sample_id"])
    sampling["round"] = new_round
    sampling["queue"] = []
    sampling["zones"] = {}
    return {
        "old_round": old_round,
        "new_round": new_round,
        "zone_ids": data["zone_ids"],
        "reason": data.get("reason", ""),
        "voided_samples": affected,
    }


def zone_status(sampling, zone_id, limit):
    """汇总某片区当前轮的达标状态。"""
    ids = sampling["zones"].get(zone_id, [])
    records = [sampling["samples"][sid] for sid in ids]
    valid = [r for r in records if r["status"] == "valid"]
    current = next((r for r in valid if r["current"]), None)
    pending = [r["sample_id"] for r in records if r["status"] in ("queued", "processing")]
    passed = current is not None and current["concentration"] <= limit
    return {
        "zone_id": zone_id,
        "round": sampling["round"],
        "passed": passed,
        "current_sample_id": current["sample_id"] if current else None,
        "current_sampled_at": current["sampled_at"] if current else None,
        "current_concentration": current["concentration"] if current else None,
        "pending_sample_ids": pending,
        "sample_ids": ids,
    }


def _current_approval(sampling, status):
    """仅当审批记录与当前轮次且当前样本版本一致时才算生效；旧审批只留档。"""
    if not status["current_sample_id"]:
        return None
    return next((a for a in sampling.get("approvals", [])
                 if a["zone_id"] == status["zone_id"]
                 and a.get("round") == sampling["round"]
                 and a.get("sample_id") == status["current_sample_id"]
                 and a.get("decision") == "approved"), None)


def pending_zones(current, limit):
    """已达标但尚未审批（或审批已被更新样本版本作废）的片区（调度员/监管视图）。"""
    sampling = current.get("sampling")
    if not sampling:
        return []
    result = []
    for zone_id in current.get("zone_ids", []):
        status = zone_status(sampling, zone_id, limit)
        if status["passed"] and _current_approval(sampling, status) is None:
            result.append(status)
    return result


def approve_restore(current, data, limit):
    """恢复审批只对当前样本版本生效；通过时固化快照，旧审批记录继续留档可查。"""
    sampling = current.get("sampling")
    if not sampling:
        raise NotFoundError("ledger_not_found", "尚未开始任何采样登记")
    zone_id = data["zone_id"]
    if zone_id not in current.get("zone_ids", []):
        raise DomainError("zone_out_of_scope", "该片区不在当前事件范围内")
    status = zone_status(sampling, zone_id, limit)
    if not status["passed"]:
        raise DomainError("quality_not_met", "该片区当前样本未达标或结果缺失，不能审批", 409)

    current_sample = sampling["samples"][status["current_sample_id"]]
    requested_sample = data.get("expected_sample_id")
    if requested_sample and requested_sample != current_sample["sample_id"]:
        raise ConflictError("sample_version_mismatch", "当前样本版本已变化，请按最新版本重新审批")

    already = _current_approval(sampling, status)
    if already is not None:
        raise ConflictError("already_approved", "该片区当前样本版本已审批通过")

    snapshot = {
        "approval_id": "APR-%d-%s" % (sampling["round"], zone_id),
        "zone_id": zone_id,
        "round": sampling["round"],
        "decision": "approved",
        "sample_id": current_sample["sample_id"],
        "sampled_at": current_sample["sampled_at"],
        "completed_at": current_sample["completed_at"],
        "valid_until": current_sample["valid_until"],
        "concentration": current_sample["concentration"],
        "limit": limit,
        "approver": data.get("actor"),
        "note": data.get("note", ""),
    }
    sampling["approvals"].append(snapshot)
    return snapshot


def all_zones_approved(current, limit):
    """返回（是否全部片区当前版本已达标并审批，未完成片区列表）。"""
    sampling = current.get("sampling")
    if not sampling:
        return False, list(current.get("zone_ids", []))
    missing = []
    for zone_id in current.get("zone_ids", []):
        status = zone_status(sampling, zone_id, limit)
        approved = _current_approval(sampling, status) is not None
        if not (status["passed"] and approved):
            missing.append(zone_id)
    return not missing, missing

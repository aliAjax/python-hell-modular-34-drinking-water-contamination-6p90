"""片区样本台账、实验室排队与恢复审批的领域逻辑。

把采样时刻、有效期、完成时间、实验室容量排队、按采样时刻推翻原结论、
区域范围变更作废、乐观并发、幂等重试、已达标未审批视图和版本化审批
合并到一本台账里。
"""

from datetime import datetime, timedelta, timezone

from .domain import DomainError

DEFAULT_VALIDITY_HOURS = 48
DEFAULT_LAB_CAPACITY = 3

SAMPLE_STATUSES = ("queued", "in_progress", "completed", "invalidated")

REGISTER_ROLES = {"lab", "field_operator", "analyst"}
COMPLETE_ROLES = {"lab", "field_operator"}
ZONE_CHANGE_ROLES = {"coordinator"}
CLEARANCE_ROLES = {"coordinator", "dispatcher", "regulator", "analyst"}
APPROVE_ROLES = {"coordinator", "regulator"}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_ts(value, name):
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value.strip()


def compute_valid_until(sampling_at, validity_hours):
    dt = datetime.fromisoformat(parse_ts(sampling_at, "sampling_at").replace("Z", "+00:00"))
    if validity_hours is None:
        hours = DEFAULT_VALIDITY_HOURS
    else:
        try:
            hours = float(validity_hours)
        except (TypeError, ValueError):
            raise DomainError("invalid_validity", "有效期必须是小时数")
    if hours <= 0:
        raise DomainError("invalid_validity", "有效期必须大于 0")
    return (dt + timedelta(hours=hours)).isoformat()


def derive_result(concentration, limit):
    if concentration is None:
        return None
    return "pass" if float(concentration) <= float(limit) else "fail"


def normalize_register(payload):
    zone_id = payload.get("zone_id")
    if not isinstance(zone_id, str) or not zone_id.strip():
        raise DomainError("field_required", "zone_id 不能为空")
    sampling_at = parse_ts(payload.get("sampling_at"), "sampling_at")

    sample_id = payload.get("sample_id")
    if sample_id is not None:
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise DomainError("invalid_sample_id", "sample_id 必须是字符串")
        sample_id = sample_id.strip()

    concentration = payload.get("concentration")
    if concentration is not None:
        if isinstance(concentration, bool):
            raise DomainError("invalid_concentration", "浓度必须是数字")
        try:
            concentration = float(concentration)
        except (TypeError, ValueError):
            raise DomainError("invalid_concentration", "浓度必须是数字")
        if concentration < 0:
            raise DomainError("invalid_concentration", "浓度不能为负数")

    validity_hours = payload.get("validity_hours", DEFAULT_VALIDITY_HOURS)
    idempotency_key = payload.get("idempotency_key")
    if idempotency_key is not None:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise DomainError("invalid_idempotency_key", "幂等键必须是非空字符串")
        idempotency_key = idempotency_key.strip()

    expected_version = payload.get("expected_version")
    if expected_version is not None:
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError):
            raise DomainError("invalid_version", "expected_version 必须是整数")

    return {
        "zone_id": zone_id.strip(),
        "sampling_at": sampling_at,
        "sample_id": sample_id,
        "concentration": concentration,
        "validity_hours": validity_hours,
        "idempotency_key": idempotency_key,
        "expected_version": expected_version,
        "note": payload.get("note", "") or "",
    }


def normalize_complete(payload):
    concentration = payload.get("concentration")
    if concentration is None:
        raise DomainError("field_required", "concentration 不能为空")
    if isinstance(concentration, bool):
        raise DomainError("invalid_concentration", "浓度必须是数字")
    try:
        concentration = float(concentration)
    except (TypeError, ValueError):
        raise DomainError("invalid_concentration", "浓度必须是数字")
    if concentration < 0:
        raise DomainError("invalid_concentration", "浓度不能为负数")
    return {"concentration": concentration, "note": payload.get("note", "") or ""}


def normalize_zone_change(payload):
    zones = payload.get("zone_ids")
    if not isinstance(zones, list) or not zones:
        raise DomainError("zones_required", "至少需要一个区域")
    if any(not isinstance(z, str) or not z.strip() for z in zones):
        raise DomainError("invalid_zones", "区域编号必须是字符串列表")
    return [z.strip() for z in zones]

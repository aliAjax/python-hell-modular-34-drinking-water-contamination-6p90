import uuid

from . import domain, rules, samples
from .domain import DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ------------------------------------------------------------------
    # 片区样本台账、实验室排队与恢复审批
    # ------------------------------------------------------------------

    def _check_role(self, role, allowed, message):
        if not role or role not in allowed:
            raise DomainError("forbidden", message, 403)

    def _region_ok(self, region, item):
        if not region or not rules.ENFORCE_REGION:
            return True
        if item["payload"].get("region") == region:
            return True
        return False

    def register_sample(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._check_role(role, samples.REGISTER_ROLES, "当前角色不能登记样本")
        norm = samples.normalize_register(payload)
        item = self.repository.get_item(item_id)
        if not self._region_ok(region, item) and role != "regulator":
            raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        valid_until = samples.compute_valid_until(norm["sampling_at"], norm["validity_hours"])
        sample_id = norm["sample_id"] or ("S-" + uuid.uuid4().hex[:12])
        capacity = int(item["payload"].get("lab_capacity", samples.DEFAULT_LAB_CAPACITY))
        now = samples.now_iso()
        return self.repository.register_sample(
            item_id, norm["zone_id"], sample_id, norm["sampling_at"], valid_until,
            norm["concentration"], capacity, norm["idempotency_key"], norm["expected_version"],
            actor, role, norm["note"], now,
        )

    def list_samples(self, item_id, zone_id=None, status=None, current_only=False):
        self.repository.get_item(item_id)
        return self.repository.list_samples(item_id, zone_id=zone_id, status=status, current_only=current_only)

    def get_sample(self, sample_pk):
        return self.repository.get_sample(sample_pk)

    def lab_queue(self, item_id, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self.repository.get_item(item_id)
        queued = self.repository.list_samples(item_id, status="queued")
        in_progress = self.repository.list_samples(item_id, status="in_progress")
        return {
            "item_id": item_id,
            "queued": queued,
            "in_progress": in_progress,
            "capacity": int(self.repository.get_item(item_id)["payload"].get("lab_capacity", samples.DEFAULT_LAB_CAPACITY)),
        }

    def complete_sample(self, sample_pk, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._check_role(role, samples.COMPLETE_ROLES, "当前角色不能完成样本检测")
        norm = samples.normalize_complete(payload)
        now = samples.now_iso()
        return self.repository.complete_sample(sample_pk, norm["concentration"], actor, role, now)

    def change_zones(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._check_role(role, samples.ZONE_CHANGE_ROLES, "当前角色不能变更区域范围")
        new_zones = samples.normalize_zone_change(payload)
        item = self.repository.get_item(item_id)
        if not self._region_ok(region, item) and role != "regulator":
            raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        now = samples.now_iso()
        updated, count = self.repository.change_zones(item_id, new_zones, actor, role, now)
        return {"item": updated, "invalidated": count}

    def clearance(self, item_id, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._check_role(role, samples.CLEARANCE_ROLES, "当前角色不能查看达标情况")
        item = self.repository.get_item(item_id)
        if not self._region_ok(region, item) and role != "regulator":
            raise DomainError("region_mismatch", "不能查看其他区域的记录", 403)
        zones = item["payload"].get("zone_ids", [])
        limit = float(item["payload"].get("limit", 0))
        ledger_version = int(item["payload"].get("sample_ledger_version", 0))
        all_samples = self.repository.list_samples(item_id)
        now = samples.now_iso()

        zone_statuses = []
        for zone in zones:
            eff = [s for s in all_samples if s["zone_id"] == zone and s["is_current"] and s["status"] != "invalidated"]
            if not eff:
                st, sample = "no_sample", None
            else:
                sample = eff[0]
                if sample["status"] != "completed":
                    st = "pending"
                elif sample["result"] == "fail":
                    st = "failed"
                elif now >= sample["valid_until"]:
                    st = "expired"
                else:
                    st = "cleared"
            zone_statuses.append({"zone_id": zone, "status": st, "sample": sample})

        all_cleared = bool(zones) and all(z["status"] == "cleared" for z in zone_statuses)
        approval = self.repository.latest_approval(item_id)
        approved = approval is not None and int(approval["sample_ledger_version"]) == ledger_version
        cleared_but_unapproved = [z for z in zone_statuses if z["status"] == "cleared" and not approved]
        return {
            "item_id": item_id,
            "zones": zone_statuses,
            "all_cleared": all_cleared,
            "approved": approved,
            "cleared_but_unapproved": cleared_but_unapproved,
            "current_version": ledger_version,
            "approval": approval,
        }

    def approve_restoration(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._check_role(role, samples.APPROVE_ROLES, "当前角色不能审批恢复")
        clr = self.clearance(item_id, actor, role, region)
        if not clr["all_cleared"]:
            raise DomainError("zones_not_cleared", "仍有区域未达标、样本未完成或已过期，不能审批恢复", 409)
        note = (payload.get("note", "") or "").strip()
        now = samples.now_iso()
        return self.repository.add_approval(item_id, actor, role, note, now)

    def list_approvals(self, item_id, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self.repository.get_item(item_id)
        return self.repository.list_approvals(item_id)

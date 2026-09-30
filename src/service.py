from . import domain, rules, ledger
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
        sampling = item["payload"].get("sampling")
        if sampling:
            limit = float(item["payload"].get("limit", 0))
            item["sampling"] = self._sampling_summary(item["payload"], limit)
        return item

    def _sampling_summary(self, payload, limit):
        sampling = payload["sampling"]
        zones = [ledger.zone_status(sampling, zone_id, limit) for zone_id in payload.get("zone_ids", [])]
        all_approved, missing = ledger.all_zones_approved(payload, limit)
        approved_ids = {a["sample_id"] for a in sampling.get("approvals", []) if a.get("decision") == "approved"}
        return {
            "round": sampling["round"],
            "lab_capacity": sampling.get("lab_capacity"),
            "queue": list(sampling["queue"]),
            "zones": zones,
            "pending_approval": [z["zone_id"] for z in zones
                                 if z["passed"] and z["current_sample_id"] not in approved_ids],
            "all_approved": all_approved,
            "missing": missing,
            "approvals": sampling.get("approvals", []),
        }

    # ---- 片区样本 / 实验室排队 / 恢复审批 ----

    def _require_ledger_role(self, role, allowed, action):
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能%s" % action, 403)

    def register_sample(self, item_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._require_ledger_role(role, ledger.REGISTER_ROLES, "登记片区样本")
        data = domain.normalize_sample_registration(payload)
        item = self.repository.get_item(item_id)
        if item["status"] not in ("disinfected", "sampled"):
            raise DomainError("invalid_state", "冲洗消毒完成后才能登记复检样本（当前 %s）" % item["status"], 409)
        capacity = data.get("lab_capacity") or ledger.DEFAULT_LAB_CAPACITY
        data["actor"] = actor

        def mutator(current):
            _, event = ledger.register_sample(current, data, capacity)
            return event

        updated, _ = self.repository.mutate_item(
            item_id, "sample_registered", actor, role, mutator, data.get("expected_version")
        )
        return self.get_item(updated["id"])

    def submit_sample_result(self, item_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._require_ledger_role(role, ledger.RESULT_ROLES, "提交实验室结果")
        data = domain.normalize_sample_result(payload)
        item = self.repository.get_item(item_id)
        limit = float(item["payload"].get("limit", 0))

        def mutator(current):
            _, event = ledger.submit_result(current, data, limit)
            return event

        updated, _ = self.repository.mutate_item(
            item_id, "sample_result_submitted", actor, role, mutator, data.get("expected_version")
        )
        return self.get_item(updated["id"])

    def change_scope(self, item_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._require_ledger_role(role, ledger.SCOPE_ROLES, "变更恢复区域范围")
        data = domain.normalize_scope_change(payload)
        item = self.repository.get_item(item_id)
        if item["status"] == "restored":
            raise DomainError("invalid_state", "已恢复供水，区域范围不能再变更", 409)

        def mutator(current):
            current["zone_ids"] = data["zone_ids"]
            return ledger.change_scope(current, data)

        updated, _ = self.repository.mutate_item(
            item_id, "scope_changed", actor, role, mutator, data["expected_version"]
        )
        return self.get_item(updated["id"])

    def approve_restore(self, item_id, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._require_ledger_role(role, ledger.APPROVE_ROLES, "审批恢复供水")
        data = domain.normalize_approval(payload)
        item = self.repository.get_item(item_id)
        if item["status"] == "restored":
            raise DomainError("invalid_state", "该事件已恢复供水", 409)
        limit = float(item["payload"].get("limit", 0))
        data["actor"] = actor

        def mutator(current):
            return ledger.approve_restore(current, data, limit)

        updated, _ = self.repository.mutate_item(
            item_id, "restore_approved", actor, role, mutator, data["expected_version"]
        )
        return self.get_item(updated["id"])

    def pending(self, item_id, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        self._require_ledger_role(role, ledger.PENDING_VIEW_ROLES, "查看待审批片区")
        item = self.repository.get_item(item_id)
        limit = float(item["payload"].get("limit", 0))
        zones = ledger.pending_zones(item["payload"], limit)
        return {"item_id": item_id, "round": item["payload"].get("sampling", {}).get("round"),
                "pending_approval": zones}

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

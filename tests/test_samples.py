import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import DomainError


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class SampleLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _create_item(self, zones=("Z-1", "Z-2"), limit=10):
        return self.service.create_item({
            "source_id": "SRC-1",
            "contaminant": "nitrate",
            "detected_at": "2026-09-27T06:00:00+00:00",
            "concentration": 20,
            "limit": limit,
            "zone_ids": list(zones),
            "population": 5000,
        }, "analyst-1", "analyst")

    def _register(self, item_id, zone, sampling_at, concentration=None, sample_id=None,
                  expected_version=None, idempotency_key=None, validity_hours=None,
                  actor="lab-1", role="lab"):
        payload = {"zone_id": zone, "sampling_at": sampling_at}
        if concentration is not None:
            payload["concentration"] = concentration
        if sample_id is not None:
            payload["sample_id"] = sample_id
        if expected_version is not None:
            payload["expected_version"] = expected_version
        if idempotency_key is not None:
            payload["idempotency_key"] = idempotency_key
        if validity_hours is not None:
            payload["validity_hours"] = validity_hours
        return self.service.register_sample(item_id, payload, actor, role)

    # 1. 登记采样时刻、有效期和完成时间
    def test_sample_records_sampling_validity_and_completion(self):
        item = self._create_item()
        sample, created = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00",
                                         concentration=2, sample_id="S-1")
        self.assertTrue(created)
        self.assertEqual(sample["sampling_at"], "2026-09-28T08:00:00+00:00")
        self.assertIsNotNone(sample["completed_at"])  # 有浓度即完成
        self.assertEqual(sample["status"], "completed")
        self.assertEqual(sample["result"], "pass")
        # 有效期 = 采样时刻 + 48h
        self.assertEqual(sample["valid_until"], "2026-09-30T08:00:00+00:00")

        # 无浓度：排队/检测中，完成时间为空
        pending, _ = self._register(item["id"], "Z-2", "2026-09-28T09:00:00+00:00",
                                    sample_id="S-2")
        self.assertIsNone(pending["completed_at"])
        self.assertIn(pending["status"], ("queued", "in_progress"))

    # 2. 实验室容量不够时先排队，完成后排队样本转入检测
    def test_lab_queue_when_capacity_full(self):
        item = self._create_item()
        s1, _ = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00", sample_id="S-1")
        s2, _ = self._register(item["id"], "Z-2", "2026-09-28T08:30:00+00:00", sample_id="S-2")
        s3, _ = self._register(item["id"], "Z-1", "2026-09-28T09:00:00+00:00", sample_id="S-3")
        s4, _ = self._register(item["id"], "Z-2", "2026-09-28T09:30:00+00:00", sample_id="S-4")
        self.assertEqual(s1["status"], "in_progress")
        self.assertEqual(s2["status"], "in_progress")
        self.assertEqual(s3["status"], "in_progress")
        self.assertEqual(s4["status"], "queued")  # 容量 3，第 4 个排队

        # 完成一个后，排队最早的转入检测
        self.service.complete_sample(s1["id"], {"concentration": 2}, "lab-1", "lab")
        promoted = self.service.get_sample(s4["id"])
        self.assertEqual(promoted["status"], "in_progress")
        completed = self.service.get_sample(s1["id"])
        self.assertEqual(completed["status"], "completed")
        self.assertIsNotNone(completed["completed_at"])

    # 3. 补交结果按采样时刻决定是否推翻原结论
    def test_supplementary_result_overturns_by_sampling_time(self):
        item = self._create_item(zones=("Z-1",), limit=10)
        a, _ = self._register(item["id"], "Z-1", "2026-09-28T09:00:00+00:00",
                              concentration=12, sample_id="S-A")
        self.assertTrue(a["is_current"])
        self.assertEqual(a["result"], "fail")

        # 晚到的补交结果（采样时刻更晚）推翻原结论
        b, _ = self._register(item["id"], "Z-1", "2026-09-28T10:00:00+00:00",
                              concentration=2, sample_id="S-B")
        self.assertTrue(b["is_current"])
        self.assertEqual(b["result"], "pass")
        self.assertFalse(self.service.get_sample(a["id"])["is_current"])

        # 更早的补交结果不推翻原结论
        c, _ = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00",
                              concentration=12, sample_id="S-C")
        self.assertFalse(c["is_current"])
        self.assertTrue(self.service.get_sample(b["id"])["is_current"])

    # 4. 区域范围在恢复前一变，现有结果作废并重新取样
    def test_zone_change_invalidates_current_samples(self):
        item = self._create_item(zones=("Z-1", "Z-2"))
        self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00", concentration=2, sample_id="S-1")
        self._register(item["id"], "Z-2", "2026-09-28T08:30:00+00:00", concentration=2, sample_id="S-2")

        result = self.service.change_zones(item["id"], {"zone_ids": ["Z-1", "Z-3"]},
                                           "coord-1", "coordinator")
        self.assertEqual(result["invalidated"], 2)
        self.assertEqual(set(result["item"]["payload"]["zone_ids"]), {"Z-1", "Z-3"})
        samples = self.service.list_samples(item["id"])
        self.assertTrue(all(s["status"] == "invalidated" for s in samples))

        clr = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertTrue(all(z["status"] == "no_sample" for z in clr["zones"]))
        self.assertFalse(clr["all_cleared"])

    # 5. 两个登记员同时提交同一片区，只留一个版本，后到者按最新版本重报
    def test_optimistic_concurrency_version_conflict(self):
        item = self._create_item(zones=("Z-1",))
        s1, _ = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00", sample_id="S-1")
        self.assertEqual(s1["version"], 1)

        # 第二个登记员读到版本 1，提交成功
        s2, _ = self._register(item["id"], "Z-1", "2026-09-28T09:00:00+00:00",
                               sample_id="S-2", expected_version=1, actor="lab-2")
        self.assertEqual(s2["version"], 2)

        # 第三个登记员用旧版本 1 提交 → 冲突
        with self.assertRaises(DomainError) as ctx:
            self._register(item["id"], "Z-1", "2026-09-28T10:00:00+00:00",
                           sample_id="S-3", expected_version=1, actor="lab-3")
        self.assertEqual(ctx.exception.code, "version_conflict")

        # 重新读取最新版本 2 后重报成功
        s3, _ = self._register(item["id"], "Z-1", "2026-09-28T10:00:00+00:00",
                               sample_id="S-3", expected_version=2, actor="lab-3")
        self.assertEqual(s3["version"], 3)

    # 6. 保存失败后保留待补样本，重试不增加样本数
    def test_idempotent_retry_does_not_duplicate(self):
        item = self._create_item(zones=("Z-1",))
        s1, created = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00",
                                     sample_id="S-1", idempotency_key="key-1")
        self.assertTrue(created)

        # 同一幂等键重试：不新增
        retry, created2 = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00",
                                          sample_id="S-1", idempotency_key="key-1")
        self.assertFalse(created2)
        self.assertEqual(retry["id"], s1["id"])

        # 业务样本号去重：重复提交不增加样本数
        retry2, created3 = self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00",
                                          sample_id="S-1")
        self.assertFalse(created3)
        self.assertEqual(retry2["id"], s1["id"])

        self.assertEqual(len(self.service.list_samples(item["id"])), 1)

        # 待补样本（排队中）保留，可重试完成
        pending, _ = self._register(item["id"], "Z-1", "2026-09-28T11:00:00+00:00",
                                    sample_id="S-2", idempotency_key="key-2")
        self.assertIsNone(pending["completed_at"])
        done, completed = self.service.complete_sample(pending["id"], {"concentration": 2},
                                                       "lab-1", "lab")
        self.assertTrue(completed)
        self.assertEqual(done["status"], "completed")
        # 重复完成幂等
        done2, completed2 = self.service.complete_sample(pending["id"], {"concentration": 2},
                                                         "lab-1", "lab")
        self.assertFalse(completed2)
        self.assertEqual(done2["id"], done["id"])

    # 7. 调度员和监管能看到已达标但未审批的片区
    def test_clearance_view_shows_cleared_but_unapproved(self):
        item = self._create_item(zones=("Z-1", "Z-2"), limit=10)
        now = _now_iso()
        self._register(item["id"], "Z-1", now, concentration=2, sample_id="S-1")
        self._register(item["id"], "Z-2", now, concentration=2, sample_id="S-2")

        clr = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertTrue(clr["all_cleared"])
        self.assertFalse(clr["approved"])
        self.assertEqual(len(clr["cleared_but_unapproved"]), 2)
        self.assertEqual({z["zone_id"] for z in clr["cleared_but_unapproved"]}, {"Z-1", "Z-2"})

        # 监管也能查看
        reg = self.service.clearance(item["id"], "reg-1", "regulator")
        self.assertTrue(reg["all_cleared"])

    # 8. 审批只对当前样本版本生效，旧记录继续可查
    def test_approval_tied_to_current_sample_version(self):
        item = self._create_item(zones=("Z-1",), limit=10)
        now = _now_iso()
        self._register(item["id"], "Z-1", now, concentration=2, sample_id="S-1")

        clr = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertTrue(clr["all_cleared"])
        self.assertFalse(clr["approved"])

        approval = self.service.approve_restoration(item["id"], {"note": "同意恢复"},
                                                    "coord-1", "coordinator")
        self.assertEqual(approval["sample_ledger_version"], 1)

        clr2 = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertTrue(clr2["approved"])
        self.assertEqual(clr2["cleared_but_unapproved"], [])

        # 新样本提交后，审批对当前版本失效
        self._register(item["id"], "Z-1", "2026-09-28T09:00:00+00:00", concentration=2, sample_id="S-2")
        clr3 = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertFalse(clr3["approved"])

        # 旧审批记录继续可查
        approvals = self.service.list_approvals(item["id"], "disp-1", "dispatcher")
        self.assertEqual(len(approvals), 1)
        self.assertEqual(approvals[0]["sample_ledger_version"], 1)
        self.assertEqual(approvals[0]["note"], "同意恢复")

    # 审批前置：所有片区达标
    def test_approval_requires_all_zones_cleared(self):
        item = self._create_item(zones=("Z-1", "Z-2"), limit=10)
        now = _now_iso()
        self._register(item["id"], "Z-1", now, concentration=2, sample_id="S-1")
        with self.assertRaises(DomainError) as ctx:
            self.service.approve_restoration(item["id"], {}, "coord-1", "coordinator")
        self.assertEqual(ctx.exception.code, "zones_not_cleared")

    # 有效期过期后不算达标
    def test_expired_sample_not_cleared(self):
        item = self._create_item(zones=("Z-1",), limit=10)
        self._register(item["id"], "Z-1", "2026-09-20T08:00:00+00:00", concentration=2,
                       sample_id="S-1", validity_hours=1)
        clr = self.service.clearance(item["id"], "disp-1", "dispatcher")
        self.assertEqual(clr["zones"][0]["status"], "expired")
        self.assertFalse(clr["all_cleared"])

    # 权限：登记员不能审批，调度员不能登记样本
    def test_role_permissions(self):
        item = self._create_item(zones=("Z-1",))
        with self.assertRaises(DomainError) as ctx:
            self.service.register_sample(item["id"], {"zone_id": "Z-1",
                                                       "sampling_at": "2026-09-28T08:00:00+00:00"},
                                         "disp-1", "dispatcher")
        self.assertEqual(ctx.exception.status, 403)

        self._register(item["id"], "Z-1", "2026-09-28T08:00:00+00:00", concentration=2, sample_id="S-1")
        with self.assertRaises(DomainError) as ctx:
            self.service.approve_restoration(item["id"], {}, "disp-1", "dispatcher")
        self.assertEqual(ctx.exception.status, 403)


if __name__ == "__main__":
    unittest.main()

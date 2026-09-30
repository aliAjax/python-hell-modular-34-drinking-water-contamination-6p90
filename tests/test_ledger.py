import os
import sys
import tempfile
import threading
import unittest
from http.client import HTTPConnection

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


def base_event(zones=("Z-1", "Z-2")):
    return {
        "source_id": "SRC-LAB",
        "contaminant": "bacteria",
        "detected_at": "2026-09-30T06:00:00+00:00",
        "concentration": 30,
        "limit": 10,
        "zone_ids": list(zones),
        "population": 5000,
        "complaints": 2,
    }


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        item = self.service.create_item(base_event(), "analyst-1", "analyst")
        self.item_id = item["id"]
        item = self.service.act(self.item_id, "verify", {"sample_count": 2}, "a", "analyst", item["version"])
        item = self.service.act(self.item_id, "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(self.item_id, "switch_source", {"alternate_source_id": "ALT-1"}, "c", "coordinator", item["version"])
        item = self.service.act(self.item_id, "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        self.item = self.service.act(self.item_id, "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])

    def tearDown(self):
        os.unlink(self.tmp.name)

    def reg(self, sample_id, zone="Z-1", sampled="2026-09-30T10:00:00+00:00", hours=24, version=None, role="lab"):
        payload = {"sample_id": sample_id, "zone_id": zone, "sampled_at": sampled,
                   "validity_hours": hours}
        if version is not None:
            payload["expected_version"] = version
        return self.service.register_sample(self.item_id, payload, "lab-1", role)

    def result(self, sample_id, completed, concentration, version=None, role="lab"):
        payload = {"sample_id": sample_id, "completed_at": completed, "concentration": concentration}
        if version is not None:
            payload["expected_version"] = version
        return self.service.submit_sample_result(self.item_id, payload, "lab-1", role)

    def test_capacity_queue_and_fifo_promotion(self):
        item = self.reg("S-1")  # capacity 默认 2
        self.assertEqual(item["sampling"]["queue"], [])
        item = self.reg("S-2", zone="Z-2")
        item = self.reg("S-3", sampled="2026-09-30T11:00:00+00:00")
        self.assertEqual(item["sampling"]["queue"], ["S-3"])
        samples = item["payload"]["sampling"]["samples"]
        self.assertEqual(samples["S-1"]["status"], "processing")
        self.assertEqual(samples["S-3"]["status"], "queued")

        item = self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        samples = item["payload"]["sampling"]["samples"]
        self.assertEqual(samples["S-3"]["status"], "processing")  # FIFO 晋升
        self.assertEqual(item["sampling"]["queue"], [])

    def test_result_for_queued_sample_rejected(self):
        self.reg("S-1")
        self.reg("S-2", zone="Z-2")
        self.reg("S-3", sampled="2026-09-30T11:00:00+00:00")
        with self.assertRaises(DomainError) as ctx:
            self.result("S-3", "2026-09-30T12:00:00+00:00", 1)
        self.assertEqual(ctx.exception.code, "sample_still_queued")
        self.assertEqual(ctx.exception.status, 409)

    def test_duplicate_submission_is_idempotent(self):
        self.reg("S-1")
        before = self.service.get_item(self.item_id)["version"]
        # 完全重复的登记：不增加样本、不增加版本
        again = self.reg("S-1")
        self.assertEqual(again["version"], before)
        self.assertEqual(len(again["payload"]["sampling"]["samples"]), 1)
        self.assertEqual(again["payload"]["sampling"]["zones"]["Z-1"], ["S-1"])
        # 完成后再补交同一样本结果被拒绝
        self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        with self.assertRaises(ConflictError):
            self.result("S-1", "2026-09-30T12:05:00+00:00", 3)

    def test_late_result_overturns_by_sampled_at_but_older_does_not(self):
        # S-1 先采样先出结果（合格）；S-2 采样更晚
        item = self.reg("S-1", sampled="2026-09-30T10:00:00+00:00")
        v = item["version"]
        item = self.reg("S-2", sampled="2026-09-30T12:00:00+00:00", version=v)
        self.result("S-1", "2026-09-30T13:00:00+00:00", 2)
        item = self.service.get_item(self.item_id)
        self.assertEqual(item["sampling"]["zones"][0]["current_sample_id"], "S-1")
        # S-2 补交不合格结果：采样时刻更晚，推翻 S-1 的达标结论
        item = self.result("S-2", "2026-09-30T15:00:00+00:00", 50)
        samples = item["payload"]["sampling"]["samples"]
        self.assertTrue(samples["S-2"]["current"])
        self.assertFalse(samples["S-1"]["current"])
        self.assertEqual(samples["S-1"]["supersedes"], "S-2")
        z1 = next(z for z in item["sampling"]["zones"] if z["zone_id"] == "Z-1")
        self.assertFalse(z1["passed"])
        # 更晚才补交一份采样时刻更早（漏送）的合格样本：不能推翻当前结论
        item = self.reg("S-0", sampled="2026-09-30T09:00:00+00:00", version=item["version"])
        item = self.result("S-0", "2026-09-30T16:00:00+00:00", 1)
        samples = item["payload"]["sampling"]["samples"]
        self.assertTrue(samples["S-2"]["current"])
        self.assertEqual(samples["S-0"]["supersedes"], "S-2")

    def test_expired_completion_is_invalid(self):
        item = self.reg("S-1", sampled="2026-09-30T10:00:00+00:00", hours=6)
        item = self.result("S-1", "2026-09-30T17:00:00+00:00", 1)
        sample = item["payload"]["sampling"]["samples"]["S-1"]
        self.assertEqual(sample["status"], "invalid")
        self.assertEqual(sample["invalid_reason"], "expired")
        self.assertFalse(item["sampling"]["zones"][0]["passed"])

    def test_scope_change_voids_round_and_requires_resampling(self):
        item = self.reg("S-1")
        item = self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        # 恢复前区域范围变化：加一个片区
        item = self.service.change_scope(self.item_id, {
            "zone_ids": ["Z-1", "Z-3"], "reason": "管网划界调整",
            "expected_version": item["version"],
        }, "c", "coordinator")
        sampling = item["payload"]["sampling"]
        self.assertEqual(sampling["round"], 2)
        self.assertTrue(sampling["samples"]["S-1"]["voided"])
        self.assertEqual(item["payload"]["zone_ids"], ["Z-1", "Z-3"])
        # 旧样本结果不能补交入账
        with self.assertRaises(ConflictError) as ctx:
            self.result("S-1", "2026-09-30T13:00:00+00:00", 2)
        self.assertEqual(ctx.exception.code, "sample_voided")
        # 重新取样
        item = self.reg("S-4", zone="Z-1", sampled="2026-09-30T14:00:00+00:00", version=item["version"])
        self.assertEqual(item["payload"]["sampling"]["samples"]["S-4"]["round"], 2)

    def test_concurrent_registration_only_one_version_kept(self):
        item = self.reg("S-1")
        v = item["version"]
        # 两个登记员拿着同一版本登记不同样本：只留一个版本，后到者 409 后按新版本重报成功
        item2 = self.reg("S-2", sampled="2026-09-30T11:00:00+00:00", version=v)
        with self.assertRaises(ConflictError) as ctx:
            self.reg("S-3", sampled="2026-09-30T11:30:00+00:00", version=v)
        self.assertEqual(ctx.exception.code, "version_conflict")
        latest = self.service.get_item(self.item_id)
        self.assertEqual(latest["version"], v + 1)
        retried = self.reg("S-3", sampled="2026-09-30T11:30:00+00:00", version=latest["version"])
        self.assertEqual(retried["version"], v + 2)

    def test_pending_view_and_approval_binds_to_current_version(self):
        item = self.reg("S-1")
        self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        # 调度员看待审批
        pending = self.service.pending(self.item_id, "d", "dispatcher")
        self.assertEqual([z["zone_id"] for z in pending["pending_approval"]], ["Z-1"])
        # 监管也可看；field_operator 不可看
        self.service.pending(self.item_id, "r", "regulator")
        with self.assertRaises(DomainError):
            self.service.pending(self.item_id, "f", "field_operator")
        item = self.service.get_item(self.item_id)
        # 用旧样本版本审批 -> 冲突
        with self.assertRaises(ConflictError):
            self.service.approve_restore(self.item_id, {
                "zone_id": "Z-1", "expected_version": item["version"], "expected_sample_id": "S-OLD",
            }, "r", "regulator")
        approved = self.service.approve_restore(self.item_id, {
            "zone_id": "Z-1", "expected_version": item["version"], "expected_sample_id": "S-1",
        }, "r", "regulator")
        snap = approved["payload"]["sampling"]["approvals"][-1]
        self.assertEqual(snap["sample_id"], "S-1")
        self.assertEqual(snap["concentration"], 2)
        # 已审批后从待审批视图消失
        pending = self.service.pending(self.item_id, "d", "dispatcher")
        self.assertNotIn("Z-1", [z["zone_id"] for z in pending["pending_approval"]])

        # 更新采样版本后，旧审批只留档，片区重新变为待审批；恢复仍被阻止
        item = self.reg("S-2", sampled="2026-09-30T13:00:00+00:00", version=approved["version"])
        self.result("S-2", "2026-09-30T14:00:00+00:00", 1)
        item = self.service.get_item(self.item_id)
        self.assertEqual(len(item["payload"]["sampling"]["approvals"]), 1)  # 旧记录继续可查
        pending = self.service.pending(self.item_id, "d", "dispatcher")
        self.assertIn("Z-1", [z["zone_id"] for z in pending["pending_approval"]])

    def test_restore_requires_all_zones_passed_and_approved(self):
        item = self.reg("S-1")  # Z-1
        self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        item = self.service.get_item(self.item_id)
        # Z-2 还没取样，恢复被拒
        with self.assertRaises(DomainError) as ctx:
            self.service.act(self.item_id, "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(ctx.exception.code, "zones_not_approved")
        item = self.reg("S-9", zone="Z-2", sampled="2026-09-30T10:30:00+00:00", version=item["version"])
        self.result("S-9", "2026-09-30T12:30:00+00:00", 3)
        item = self.service.get_item(self.item_id)
        self.service.approve_restore(self.item_id, {"zone_id": "Z-1", "expected_version": item["version"]}, "c", "coordinator")
        item = self.service.get_item(self.item_id)
        self.service.approve_restore(self.item_id, {"zone_id": "Z-2", "expected_version": item["version"]}, "r", "regulator")
        item = self.service.get_item(self.item_id)
        restored = self.service.act(self.item_id, "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(restored["status"], "restored")

    def test_approval_permissions(self):
        self.reg("S-1")
        self.result("S-1", "2026-09-30T12:00:00+00:00", 2)
        item = self.service.get_item(self.item_id)
        with self.assertRaises(DomainError):  # lab 不能审批
            self.service.approve_restore(self.item_id, {"zone_id": "Z-1", "expected_version": item["version"]}, "l", "lab")

    def test_failed_save_then_retry_keeps_pending_sample(self):
        # 模拟首次提交后调用方未收到响应：携带相同 sample_id 重试，样本仍只保留一份
        item = self.reg("S-1")
        first_version = item["version"]
        retry = self.reg("S-1")
        self.assertEqual(retry["version"], first_version)
        self.assertEqual(len(retry["payload"]["sampling"]["samples"]), 1)


class LedgerHttpTest(unittest.TestCase):
    """端到端 HTTP：并发登记与待审批视图。"""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        from src.http_api import build_handler
        from http.server import ThreadingHTTPServer
        import os as _os
        static_dir = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "static")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(self.service, static_dir))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        def req(method, path, body, role="analyst", user="a"):
            conn = HTTPConnection("127.0.0.1", self.port, timeout=10)
            import json
            headers = {"Content-Type": "application/json", "X-User-Id": user, "X-Role": role}
            conn.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
            resp = conn.getresponse()
            data = json.loads(resp.read().decode("utf-8"))
            conn.close()
            return resp.status, data

        self.req = req
        status, item = req("POST", "/api/items", base_event())
        self.item_id = item["id"]
        v = item["version"]
        for action, body, role, user in [
            ("verify", {"sample_count": 2}, "analyst", "a"),
            ("advise", {"notice_id": "N-1", "kind": "boil", "message": "x"}, "dispatcher", "d"),
            ("switch_source", {"alternate_source_id": "ALT"}, "coordinator", "c"),
            ("flush", {"zone_id": "Z-1"}, "field_operator", "f"),
            ("disinfect", {"zone_id": "Z-1", "completed": True}, "field_operator", "f"),
        ]:
            body["expected_version"] = v
            status, out = req("POST", "/api/items/%d/actions" % self.item_id, {"action": action, **body}, role, user)
            self.assertEqual(status, 200, out)
            v = out["version"]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        os.unlink(self.tmp.name)

    def test_concurrent_register_http(self):
        import threading
        results = []

        def register(sid):
            status, data = self.req("POST", "/api/items/%d/sampling/register" % self.item_id, {
                "sample_id": sid, "zone_id": "Z-1",
                "sampled_at": "2026-09-30T1%d:00:00+00:00" % int(sid[-1]),
                "validity_hours": 24,
            }, role="lab")
            results.append((sid, status, data))

        threads = [threading.Thread(target=register, args=("S-%d" % i,)) for i in range(1, 4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 所有并发登记都被接受（每份样本唯一），版本连续无丢失
        self.assertTrue(all(status == 201 for _, status, _ in results), results)
        status, item = self.req("GET", "/api/items/%d" % self.item_id, None, role="lab")
        self.assertEqual(len(item["payload"]["sampling"]["samples"]), 3)

    def test_pending_endpoint_permission(self):
        self.req("POST", "/api/items/%d/sampling/register" % self.item_id, {
            "sample_id": "S-1", "zone_id": "Z-1",
            "sampled_at": "2026-09-30T10:00:00+00:00", "validity_hours": 24,
        }, role="lab")
        self.req("POST", "/api/items/%d/sampling/results" % self.item_id, {
            "sample_id": "S-1", "completed_at": "2026-09-30T12:00:00+00:00", "concentration": 2,
        }, role="lab")
        status, data = self.req("GET", "/api/items/%d/sampling/pending" % self.item_id, None, role="dispatcher", user="d")
        self.assertEqual(status, 200)
        self.assertEqual([z["zone_id"] for z in data["pending_approval"]], ["Z-1"])
        status, data = self.req("GET", "/api/items/%d/sampling/pending" % self.item_id, None, role="lab")
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()

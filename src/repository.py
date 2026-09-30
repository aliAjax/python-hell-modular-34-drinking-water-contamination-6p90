import json
import sqlite3
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError
from .samples import derive_result


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    zone_id TEXT NOT NULL,
                    sample_id TEXT NOT NULL,
                    sampling_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL,
                    completed_at TEXT,
                    concentration REAL,
                    result TEXT,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    is_current INTEGER NOT NULL DEFAULT 0,
                    idempotency_key TEXT,
                    submitted_by TEXT NOT NULL,
                    submitted_role TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    invalidated_at TEXT,
                    invalidated_reason TEXT,
                    UNIQUE(item_id, sample_id),
                    UNIQUE(item_id, idempotency_key),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE INDEX IF NOT EXISTS idx_samples_item_zone ON samples(item_id, zone_id);
                CREATE INDEX IF NOT EXISTS idx_samples_item_status ON samples(item_id, status);
                CREATE TABLE IF NOT EXISTS restoration_approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    sample_ledger_version INTEGER NOT NULL,
                    approved_by TEXT NOT NULL,
                    approved_role TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                """
            )
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 片区样本台账
    # ------------------------------------------------------------------

    def _sample_to_row(self, row):
        if row is None:
            return None
        result = dict(row)
        result["is_current"] = bool(result["is_current"])
        return result

    def _bump_ledger_version(self, conn, item_id, now):
        row = conn.execute("SELECT payload FROM items WHERE id=?", (item_id,)).fetchone()
        payload = json.loads(row["payload"])
        payload["sample_ledger_version"] = int(payload.get("sample_ledger_version", 0)) + 1
        conn.execute("UPDATE items SET payload=?, updated_at=? WHERE id=?", (canonical_json(payload), now, item_id))
        return payload["sample_ledger_version"]

    def _recompute_current(self, conn, item_id, zone_id, now):
        # 有效样本 = 未作废样本中采样时刻最新者；按采样时刻决定是否推翻原结论
        row = conn.execute(
            "SELECT id FROM samples WHERE item_id=? AND zone_id=? AND status!='invalidated' "
            "ORDER BY sampling_at DESC, id DESC LIMIT 1",
            (item_id, zone_id),
        ).fetchone()
        conn.execute(
            "UPDATE samples SET is_current=0, updated_at=? WHERE item_id=? AND zone_id=?",
            (now, item_id, zone_id),
        )
        if row is not None:
            conn.execute("UPDATE samples SET is_current=1, updated_at=? WHERE id=?", (now, row["id"]))
        return row["id"] if row else None

    def next_sample_version(self, conn, item_id, zone_id):
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM samples WHERE item_id=? AND zone_id=?",
            (item_id, zone_id),
        ).fetchone()
        return int(row["v"]) + 1

    def count_in_progress(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM samples WHERE item_id=? AND status='in_progress'",
                (item_id,),
            ).fetchone()
            return int(row["c"])
        finally:
            conn.close()

    def get_sample(self, sample_pk):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM samples WHERE id=?", (sample_pk,)).fetchone()
            if row is None:
                raise NotFoundError("sample_not_found", "样本不存在")
            return self._sample_to_row(row)
        finally:
            conn.close()

    def list_samples(self, item_id, zone_id=None, status=None, current_only=False):
        conn = self.connect()
        try:
            sql = "SELECT * FROM samples WHERE item_id=?"
            params = [item_id]
            if zone_id:
                sql += " AND zone_id=?"
                params.append(zone_id)
            if status:
                sql += " AND status=?"
                params.append(status)
            if current_only:
                sql += " AND is_current=1"
            sql += " ORDER BY id DESC"
            rows = conn.execute(sql, params).fetchall()
            return [self._sample_to_row(row) for row in rows]
        finally:
            conn.close()

    def register_sample(self, item_id, zone_id, sample_id, sampling_at, valid_until, concentration,
                        capacity, idempotency_key, expected_version, actor, role, note, now):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            limit = float(json.loads(item["payload"]).get("limit", 0))

            # 幂等：保存失败后重试同一幂等键，不新增样本
            if idempotency_key:
                existing = conn.execute(
                    "SELECT * FROM samples WHERE item_id=? AND idempotency_key=?",
                    (item_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    conn.execute("COMMIT")
                    return self._sample_to_row(existing), False

            # 业务样本号去重：重复提交不增加样本数
            existing = conn.execute(
                "SELECT * FROM samples WHERE item_id=? AND sample_id=?",
                (item_id, sample_id),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                return self._sample_to_row(existing), False

            # 乐观并发：两个登记员同时提交同一片区，后到者按最新版本重报
            current_version = self.next_sample_version(conn, item_id, zone_id) - 1
            if expected_version is not None and int(expected_version) != current_version:
                raise ConflictError("version_conflict", "样本版本已更新，请重新读取最新版本后重报")

            version = current_version + 1
            if concentration is not None:
                status = "completed"
                completed_at = now
                result = derive_result(concentration, limit)
            else:
                completed_at = None
                result = None
                in_progress = conn.execute(
                    "SELECT COUNT(*) AS c FROM samples WHERE item_id=? AND status='in_progress'",
                    (item_id,),
                ).fetchone()["c"]
                status = "in_progress" if int(in_progress) < int(capacity) else "queued"

            conn.execute(
                "INSERT INTO samples(item_id,zone_id,sample_id,sampling_at,valid_until,completed_at,"
                "concentration,result,status,version,is_current,idempotency_key,submitted_by,"
                "submitted_role,note,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (item_id, zone_id, sample_id, sampling_at, valid_until, completed_at, concentration,
                 result, status, version, 0, idempotency_key, actor, role, note, now, now),
            )
            pk = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self._recompute_current(conn, item_id, zone_id, now)
            self._bump_ledger_version(conn, item_id, now)
            self.append_audit(
                conn, item_id, "sample_registered", actor, role,
                {"sample_id": sample_id, "zone_id": zone_id, "version": version,
                 "status": status, "sampling_at": sampling_at},
            )
            conn.execute("COMMIT")
            return self.get_sample(pk), True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def complete_sample(self, sample_pk, concentration, actor, role, now):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM samples WHERE id=?", (sample_pk,)).fetchone()
            if row is None:
                raise NotFoundError("sample_not_found", "样本不存在")
            if row["status"] == "invalidated":
                raise DomainError("sample_invalidated", "样本已作废，不能完成检测")
            if row["status"] == "completed":
                conn.execute("COMMIT")
                return self._sample_to_row(row), False
            limit = float(json.loads(conn.execute("SELECT payload FROM items WHERE id=?", (row["item_id"],)).fetchone()["payload"]).get("limit", 0))
            result = derive_result(concentration, limit)
            conn.execute(
                "UPDATE samples SET concentration=?, result=?, status='completed', completed_at=?, updated_at=? WHERE id=?",
                (concentration, result, now, now, sample_pk),
            )
            # 容量释放后，排队中最早的样本转入检测
            next_queued = conn.execute(
                "SELECT id FROM samples WHERE item_id=? AND status='queued' ORDER BY id ASC LIMIT 1",
                (row["item_id"],),
            ).fetchone()
            if next_queued is not None:
                conn.execute(
                    "UPDATE samples SET status='in_progress', updated_at=? WHERE id=?",
                    (now, next_queued["id"]),
                )
            self._recompute_current(conn, row["item_id"], row["zone_id"], now)
            self._bump_ledger_version(conn, row["item_id"], now)
            self.append_audit(
                conn, row["item_id"], "sample_completed", actor, role,
                {"sample_id": row["sample_id"], "zone_id": row["zone_id"],
                 "concentration": concentration, "result": result},
            )
            conn.execute("COMMIT")
            return self.get_sample(sample_pk), True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def change_zones(self, item_id, new_zone_ids, actor, role, now):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            payload = json.loads(row["payload"])
            old_zones = list(payload.get("zone_ids", []))
            payload["zone_ids"] = list(new_zone_ids)
            # 区域范围一变，现有结果全部作废并重新取样
            cur = conn.execute(
                "SELECT id FROM samples WHERE item_id=? AND status!='invalidated'",
                (item_id,),
            ).fetchall()
            count = 0
            for sample_row in cur:
                conn.execute(
                    "UPDATE samples SET status='invalidated', is_current=0, invalidated_at=?, "
                    "invalidated_reason=?, updated_at=? WHERE id=?",
                    (now, "zone_range_changed", now, sample_row["id"]),
                )
                count += 1
            payload["sample_ledger_version"] = int(payload.get("sample_ledger_version", 0)) + 1
            conn.execute("UPDATE items SET payload=?, updated_at=? WHERE id=?",
                        (canonical_json(payload), now, item_id))
            self.append_audit(
                conn, item_id, "zone_range_changed", actor, role,
                {"old_zones": old_zones, "new_zones": list(new_zone_ids), "invalidated": count},
            )
            conn.execute("COMMIT")
            return self.get_item(item_id), count
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def add_approval(self, item_id, actor, role, note, now):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            payload = json.loads(row["payload"])
            ledger_version = int(payload.get("sample_ledger_version", 0))
            conn.execute(
                "INSERT INTO restoration_approvals(item_id,sample_ledger_version,approved_by,"
                "approved_role,note,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, ledger_version, actor, role, note, now),
            )
            pk = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn, item_id, "restoration_approved", actor, role,
                {"sample_ledger_version": ledger_version, "note": note},
            )
            conn.execute("COMMIT")
            return self.get_approval(pk)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_approval(self, approval_pk):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM restoration_approvals WHERE id=?", (approval_pk,)).fetchone()
            if row is None:
                raise NotFoundError("approval_not_found", "恢复审批不存在")
            return dict(row)
        finally:
            conn.close()

    def latest_approval(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM restoration_approvals WHERE item_id=? ORDER BY id DESC LIMIT 1",
                (item_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_approvals(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM restoration_approvals WHERE item_id=? ORDER BY id DESC",
                (item_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

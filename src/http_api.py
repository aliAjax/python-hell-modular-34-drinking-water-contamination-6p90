import json
import mimetypes
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from .domain import DomainError


def build_handler(service, static_dir):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ModularPythonHell/1.0"

        def log_message(self, fmt, *args):
            return

        def _identity(self):
            actor = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            region = self.headers.get("X-Region", "").strip() or None
            return actor, role, region

        def _json_body(self):
            length = int(self.headers.get("Content-Length", "0") or "0")
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise DomainError("invalid_json", "请求体不是有效 JSON", 400)

        def _send(self, status, value, content_type="application/json; charset=utf-8"):
            if not isinstance(value, (bytes, bytearray)):
                value = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(value)))
            self.end_headers()
            self.wfile.write(value)

        def _error(self, exc):
            status = getattr(exc, "status", 500)
            code = getattr(exc, "code", "internal_error")
            self._send(status, {"error": code, "message": str(exc)})

        def do_GET(self):
            try:
                actor, role, region = self._identity()
                path = urlparse(self.path).path
                if path == "/health":
                    return self._send(200, {"status": "ok"})
                if path == "/api/state":
                    return self._send(200, service.state())
                if path == "/api/items":
                    return self._send(200, {"items": service.list_items()})
                parts = [part for part in path.split("/") if part]
                if len(parts) == 3 and parts[:2] == ["api", "items"]:
                    return self._send(200, service.get_item(int(parts[2])))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "audit":
                    item = service.get_item(int(parts[2]))
                    return self._send(200, {"events": item["audit"]})
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "samples":
                    query = parse_qs(urlparse(self.path).query)
                    zone_id = query.get("zone_id", [None])[0]
                    status = query.get("status", [None])[0]
                    current_only = query.get("current", ["0"])[0] in ("1", "true", "yes")
                    return self._send(200, {"samples": service.list_samples(int(parts[2]), zone_id, status, current_only)})
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "lab-queue":
                    return self._send(200, service.lab_queue(int(parts[2]), actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "clearance":
                    return self._send(200, service.clearance(int(parts[2]), actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "restoration-approvals":
                    return self._send(200, {"approvals": service.list_approvals(int(parts[2]), actor, role, region)})
                if path == "/":
                    file_path = os.path.join(static_dir, "index.html")
                    with open(file_path, "rb") as handle:
                        content = handle.read()
                    return self._send(200, content, "text/html; charset=utf-8")
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except (ValueError, OSError) as exc:
                return self._error(DomainError("invalid_request", str(exc), 400))

        def do_POST(self):
            actor = role = region = None
            try:
                actor, role, region = self._identity()
                path = urlparse(self.path).path
                payload = self._json_body()
                parts = [part for part in path.split("/") if part]
                if parts == ["api", "items"]:
                    return self._send(201, service.create_item(payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "sources":
                    return self._send(201, service.add_source(int(parts[2]), payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "actions":
                    action = payload.pop("action", "")
                    if not action:
                        raise DomainError("action_required", "缺少 action", 400)
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.act(int(parts[2]), action, payload, actor, role, expected, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "samples":
                    sample, created = service.register_sample(int(parts[2]), payload, actor, role, region)
                    return self._send(201 if created else 200, {"sample": sample, "created": created})
                if len(parts) == 6 and parts[:2] == ["api", "items"] and parts[3] == "samples" and parts[5] == "complete":
                    sample, completed = service.complete_sample(int(parts[4]), payload, actor, role, region)
                    return self._send(200, {"sample": sample, "completed": completed})
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "zones":
                    result = service.change_zones(int(parts[2]), payload, actor, role, region)
                    return self._send(200, result)
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "restoration-approvals":
                    approval = service.approve_restoration(int(parts[2]), payload, actor, role, region)
                    return self._send(201, {"approval": approval})
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except Exception as exc:
                return self._error(DomainError("internal_error", str(exc), 500))

    return Handler

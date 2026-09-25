"""女性脑影像队列复现的运行入口。

HTTP 层只负责路由与序列化，领域逻辑在 ``recompute.Platform``。
"""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from recompute import DomainError, Platform, Store

SERVICE_ID = "brain-cohort-reproduction"
SERVICE_NAME = "女性脑影像队列复现"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def build_platform(state_path=None):
    return Platform(Store(state_path))


# 路由表：method -> [(路径片段, 处理函数名)]
# {id} 为单段占位符。
_ROUTES = {
    "GET": [
        ("/health", "health"),
        ("/devices/{id}/lineage", "device_lineage"),
        ("/contradictions", "list_contradictions"),
        ("/plans/{id}", "get_plan"),
        ("/analyses/{id}", "get_analysis"),
        ("/metrics/{id}/explain", "explain_metric"),
    ],
    "POST": [
        ("/devices", "register_device"),
        ("/calibration-notices", "register_notice"),
        ("/contradictions/{id}/adjudicate", "adjudicate"),
        ("/participants:batch", "register_participants"),
        ("/scans:batch", "register_scans"),
        ("/metrics:batch", "register_metrics"),
        ("/analyses:batch", "register_analyses"),
        ("/plans", "create_plan"),
        ("/plans/{id}/approvals", "approve_plan"),
        ("/plans/{id}/execute", "execute_plan"),
        ("/errata/{id}/publish", "publish_erratum"),
    ],
}


def _match(pattern, path):
    pattern_parts = [p for p in pattern.split("/") if p]
    path_parts = [p for p in path.split("/") if p]
    if len(pattern_parts) != len(path_parts):
        return None
    kwargs = {}
    for pat, part in zip(pattern_parts, path_parts):
        if pat.startswith("{") and pat.endswith("}"):
            kwargs[pat[1:-1]] = part
        elif pat != part:
            return None
    return kwargs


class Handler(BaseHTTPRequestHandler):
    """领域接口与健康检查的统一入口。"""

    platform = None  # 由 main/测试在类上注入共享实例

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        path = urlparse(self.path).path
        for pattern, action in _ROUTES[method]:
            kwargs = _match(pattern, path)
            if kwargs is not None:
                return self._run_action(action, kwargs)
        self.send_error(404)

    def _run_action(self, action, kwargs):
        payload = {}
        if self.command == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            if raw:
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    self._write_json(400, {"error": {"code": "invalid_json", "message": "请求体不是合法 JSON"}})
                    return
        try:
            result = getattr(self, f"_action_{action}")(payload, **kwargs)
        except DomainError as error:
            self._write_json(
                error.status,
                {"error": {"code": error.code, "message": error.message, "details": error.details}},
            )
            return
        self._write_json(200, result)

    def _write_json(self, status, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---------- 动作 ----------

    def _action_health(self, _payload):
        return health_payload()

    def _action_register_device(self, payload):
        return self.platform.register_device(payload)

    def _action_device_lineage(self, _payload, id):
        return self.platform.device_lineage(id)

    def _action_register_notice(self, payload):
        return self.platform.register_calibration_notice(payload)

    def _action_list_contradictions(self, _payload):
        return {"contradictions": self.platform.open_contradictions()}

    def _action_adjudicate(self, payload, id):
        return self.platform.adjudicate_contradiction(id, payload)

    def _action_register_participants(self, payload):
        return self.platform.register_participants(
            payload.get("participants", payload if isinstance(payload, list) else [])
        )

    def _action_register_scans(self, payload):
        return self.platform.register_scans(payload.get("scans", payload if isinstance(payload, list) else []))

    def _action_register_metrics(self, payload):
        return self.platform.register_metrics(payload.get("metrics", payload if isinstance(payload, list) else []))

    def _action_register_analyses(self, payload):
        return self.platform.register_analyses(payload.get("analyses", payload if isinstance(payload, list) else []))

    def _action_create_plan(self, payload):
        return self.platform.create_plan(payload)

    def _action_get_plan(self, _payload, id):
        return self.platform.get_plan(id)

    def _action_approve_plan(self, payload, id):
        return self.platform.approve_plan(id, payload)

    def _action_execute_plan(self, _payload, id):
        return self.platform.execute_plan(id)

    def _action_publish_erratum(self, payload, id):
        return self.platform.publish_erratum(id, payload)

    def _action_get_analysis(self, _payload, id):
        return self.platform.get_analysis(id)

    def _action_explain_metric(self, _payload, id):
        return self.platform.explain_metric(id)

    def log_message(self, *_args):
        return


def make_handler(platform):
    """构造绑定了共享平台实例的 Handler 子类。"""
    return type("BoundHandler", (Handler,), {"platform": platform})


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--state", default=None, help="JSON 状态文件路径，缺省为纯内存")
    args = parser.parse_args()
    if args.check:
        platform = build_platform(args.state)
        assert health_payload()["service"] == SERVICE_ID
        assert isinstance(platform, Platform)
        print("基础检查通过")
        return
    platform = build_platform(args.state)
    ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(platform)).serve_forever()


if __name__ == "__main__":
    main()

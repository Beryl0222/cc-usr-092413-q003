"""女性脑影像队列复现的运行入口。

除健康检查外，对外暴露校准漂移重算相关的 JSON 接口，领域规则见
``recalc`` 模块。服务只依赖标准库，状态默认保存在内存，可用
``--db`` 指定 JSON 快照文件持久化。
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from recalc import (
    RecalcError,
    RecalcStore,
    approve_plan,
    create_plan,
    device_lineage,
    explain_metric,
    get_plan,
    register_comparison,
    register_consent,
    register_device,
    register_metric,
    register_site,
    register_visit,
    register_withdrawal,
    resolve_calibration_conflict,
    run_plan,
    submit_calibration_notice,
)

SERVICE_ID = "brain-cohort-reproduction"
SERVICE_NAME = "女性脑影像队列复现"

# POST 路由到领域函数的分发表。
POST_ROUTES = {
    "/admin/sites": register_site,
    "/admin/devices": register_device,
    "/visits": register_visit,
    "/consents": register_consent,
    "/withdrawals": register_withdrawal,
    "/metrics": register_metric,
    "/comparisons": register_comparison,
    "/calibration/notices": submit_calibration_notice,
    "/calibration/conflicts/resolve": resolve_calibration_conflict,
    "/recalc/plans": create_plan,
}


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def create_handler(store):
    """构造绑定指定存储的 Handler 类，便于测试隔离。"""

    class Handler(BaseHTTPRequestHandler):
        """提供健康检查与重算领域接口。"""

        def _write_json(self, status, payload):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RecalcError(f"请求体不是合法 JSON: {exc}") from exc
            if not isinstance(payload, dict):
                raise RecalcError("请求体必须是 JSON 对象")
            return payload

        def do_GET(self):
            path = self.path
            if path == "/health":
                self._write_json(200, health_payload())
                return
            parts = [segment for segment in path.split("/") if segment]
            try:
                if len(parts) == 3 and parts[0] == "devices" and parts[2] == "lineage":
                    self._write_json(200, device_lineage(store, parts[1]))
                    return
                if len(parts) == 3 and parts[0] == "recalc" and parts[1] == "plans":
                    self._write_json(200, get_plan(store, parts[2]))
                    return
                if len(parts) == 3 and parts[0] == "metrics" and parts[2] == "explain":
                    self._write_json(200, explain_metric(store, parts[1]))
                    return
            except RecalcError as exc:
                self._write_json(400, {"error": str(exc)})
                return
            self.send_error(404)

        def do_POST(self):
            path = self.path
            try:
                payload = self._read_json()
            except RecalcError as exc:
                self._write_json(400, {"error": str(exc)})
                return

            try:
                if path in POST_ROUTES:
                    self._write_json(200, POST_ROUTES[path](store, payload))
                    return
                # 带路径参数的动作接口。
                parts = [segment for segment in path.split("/") if segment]
                if (
                    len(parts) == 4
                    and parts[:2] == ["recalc", "plans"]
                    and parts[3] == "approvals"
                ):
                    self._write_json(
                        200,
                        approve_plan(
                            store,
                            parts[2],
                            payload.get("role"),
                            payload.get("approver"),
                            payload.get("evidence"),
                        ),
                    )
                    return
                if (
                    len(parts) == 4
                    and parts[:2] == ["recalc", "plans"]
                    and parts[3] == "run"
                ):
                    self._write_json(
                        200, run_plan(store, parts[2], payload.get("fail_after"))
                    )
                    return
            except RecalcError as exc:
                self._write_json(400, {"error": str(exc)})
                return
            self.send_error(404)

        def log_message(self, *_args):
            return

    return Handler


# 默认内存存储，保持 ``Handler`` 可直接实例化（契约测试依赖）。
STORE = RecalcStore()
Handler = create_handler(STORE)


def build_server(host, port, store=None):
    handler_store = store if store is not None else STORE
    return ThreadingHTTPServer((host, port), create_handler(handler_store))


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--db", help="JSON 快照持久化路径，缺省为纯内存")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    store = RecalcStore(args.db) if args.db else STORE
    build_server("0.0.0.0", args.port, store).serve_forever()


if __name__ == "__main__":
    main()

# 女性脑影像队列复现

服务用于管理女性人生阶段研究的访视、影像、同意与分析快照，支持对结论适用范围的复核。

项目当前提供稳定的基础服务入口，便于本地联调和运维巡检。运行 `python3 service.py --check` 可核对服务配置；执行 `python3 service.py --port 8000` 后访问 `/health` 可确认服务身份。

## 测试与构建

执行完整测试：

```bash
npm test
```

执行编译检查：

```bash
python3 -m compileall -q .
```

两条命令都可在单个 Linux 应用容器内直接运行，不需要额外服务。

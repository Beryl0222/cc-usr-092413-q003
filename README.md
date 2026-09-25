# 女性脑影像队列复现

服务用于管理女性人生阶段研究的访视、影像、同意与分析快照，支持对结论适用范围的复核，并在线圈更换等
校准事件后，**只重算受影响中心、序列与派生指标**，不触碰未受影响中心的冻结结果。

运行 `python3 service.py --check` 可核对服务配置；执行 `python3 service.py --port 8000` 后访问 `/health`
可确认服务身份。加 `--state state.json` 可将谱系、计划与结果原子落盘（缺省为纯内存，便于联调）。

## 领域流程

1. **谱系登记**：`POST /devices` 登记扫描设备；`POST /calibration-notices` 推送校准通告。
   每台设备的校准区间形成左闭右开的版本链（线圈更换自动关闭前序区间）。
2. **通告去重与矛盾暂停**：同设备、同生效时点、同参数的通告幂等忽略，不重复排队；
   参数不一致则区间置为 `pending_adjudication`，相关重算被暂停，
   `GET /contradictions` 可见待裁决项，`POST /contradictions/{id}/adjudicate` 选定权威版本后解除。
3. **影响判定与计划**：`POST /plans` 按采集时点落在区间内的扫描确定受影响访视，
   计划范围仅含相关中心、序列与派生指标；已撤回或缺少 `recompute` 同意范围的参与者数据在计划阶段
   即排除（执行前再次校验，撤回即时生效）。
4. **双闸门审批**：质控负责人确认漂移证据、统计负责人批准分析影响后计划才可执行
   （`POST /plans/{id}/approvals`，角色 `qc` / `statistician`）。
5. **检查点执行**：`POST /plans/{id}/execute` 逐单元提交，管线故障时计划暂停；
   恢复后从最后完成单元继续，已完成单元不重复计算。
6. **分析处理**：已冻结（锁库后发表）分析保留原快照，另生成**勘误候选**连接新结果
   （`POST /errata/{id}/publish` 发布）；未冻结分析在全部受影响输入就绪后**原子替换**，
   替换记录含阶段比较的前后向量。
7. **解释查询**：`GET /metrics/{id}/explain` 说明指标是否重算、为何保持原值
   （撤回 / 同意不足 / 区间未裁决 / 计划待批 / 未受影响）、采用的校准版本，
   以及对各人生阶段比较（冻结分析的勘误或未冻结分析的前后向量）的影响。

其他只读接口：`GET /devices/{id}/lineage`、`GET /plans/{id}`、`GET /analyses/{id}`。
批量登记接口：`POST /participants:batch`、`/scans:batch`、`/metrics:batch`、`/analyses:batch`，
请求体为 `{"...": [记录, ...]}` 或直接传记录数组。

## 设计约束

- 重算管线通过 `Platform(recompute_fn=...)` 注入；默认实现按校准参数 `correction_factor` 修正数值，
  仅用于联调。
- 状态存储为单文件 JSON，写操作经临时文件 + `os.replace` 原子替换；进程内写操作由单锁串行化。
- 领域错误统一携带稳定错误码（如 `plan_already_queued`、`interval_pending_adjudication`、
  `plan_not_approved`、`consent_scope_insufficient`），HTTP 层映射为 4xx。

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

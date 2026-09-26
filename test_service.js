"use strict";

const { spawnSync } = require("node:child_process");

// 基础服务契约 + 校准漂移重算领域契约。
const result = spawnSync(
  "python3",
  ["-m", "unittest", "-v", "service_contract", "recalc_contract"],
  { stdio: "inherit" }
);
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);

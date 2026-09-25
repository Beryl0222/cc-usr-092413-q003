"use strict";

const { spawnSync } = require("node:child_process");

const modules = ["service_contract", "test_recompute"];

let failed = false;
for (const mod of modules) {
  const result = spawnSync("python3", ["-m", "unittest", "-v", mod], { stdio: "inherit" });
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  if (result.status !== 0) {
    failed = true;
  }
}
process.exit(failed ? 1 : 0);

# gb-1037 验证证据：回波分批与按批增益更正

本目录与源码分离，保存本次需求的端到端验证脚本。环境无 Docker，使用 SQLite +
fakeredis 替代 compose 内的 PostgreSQL/Redis，业务代码（含真实 worker 的
`process_once`）不改，仅替换队列连接与数据库 URL。

## 运行

```sh
# 依赖装在独立虚拟环境（仓库外），不污染工作区
uv venv --python 3.12 /tmp/venv312
uv pip install --python /tmp/venv312/bin/python -r backend/requirements.txt fakeredis httpx
DATABASE_URL="sqlite:////tmp/gb1037.db" /tmp/venv312/bin/python -I test-artifacts/gb-1037/test_batches.py
DATABASE_URL="sqlite:////tmp/gb1037r.db" /tmp/venv312/bin/python -I test-artifacts/gb-1037/test_regression.py
```

## 覆盖的业务规则（test_batches.py，35 项）

- 同一航段分多批录入，航段内批次号唯一；留空自动递增；重复批次号 409。
- 每批记录当时的探头增益；按批更正只写 `corrected_gain_db`，
  `recorded_gain_db` 与 `echoes.strength_db` 原读数永不被覆盖。
- 早期未分批回波在启动时幂等回填为第 1 批，读数不变。
- 更正落账即 `data_version+1`，旧识别全部过期；旧版本结果即使晚算完也不能复核
  （在途旧任务 worker 直接 failed，复核接口 409）。
- 全段重算只调整被更正批次的输入：`有效强度 = 原读数 -(更正增益-原始增益)`，
  其他批次保持原读数；`input_sha256` 随之变化。
- 点位归类与指标在同一事务齐备后才置 done，半成品不可复核。
- 重试/重复更正不叠加增益差（始终以原始增益为基准重算差值）。
- 封存航次拒绝更正；viewer 只读；增益取值范围 10–80dB 校验。

## 回归（test_regression.py，23 项）

种子航次走通完整剧本：改航窗→开航→模拟回波（自动成批）→真实 DBSCAN 识别→
噪点复核→网具复校→生物样→缺测补航全链路→E2 补齐→返航→图层封存。
封存快照每层含 `batches`，回波仍保留原始 `strength_db`，canonical JSON 的
UTF-8 SHA256 与 `json.loads` 还原一致性保持成立；封存后更正被拒绝。

本地运行时 `/artifacts` 卷不可写，worker 对该落盘失败降级为告警，不影响
识别结果落库；容器内命名卷可写，行为与原来一致。

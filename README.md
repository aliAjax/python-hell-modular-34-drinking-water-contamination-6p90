# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域和来源校验。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、冲洗、消毒、复检和恢复状态机。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本和审计链。
- `src/service.py`：身份、角色和用例编排。
- `src/ledger.py`：片区样本登记、实验室容量排队、按采样时刻对账和恢复审批快照。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions`、`GET /api/items/<id>/sampling/pending`、`POST /api/items/<id>/sampling/register|results|scope|approve` 和审计查询。

### 复检样本与恢复审批（冲洗消毒完成后）

- `POST /sampling/register`（lab/field_operator/analyst/dispatcher）：登记 `sample_id`、`zone_id`、`sampled_at`、`valid_until` 或 `validity_hours`；可在首份样本携带 `lab_capacity` 设置实验室容量。满员时样本进入 `queued`，出结果后按 FIFO 晋升。重复提交相同 `sample_id` 幂等，保存失败后重试不增加样本数；并发登记通过 `expected_version` 乐观控制，只留一个版本，后到者拿最新版本重报。
- `POST /sampling/results`（lab）：补交 `completed_at` 与 `concentration`。完成时间晚于 `valid_until` 记为过期无效；同一轮内按 `sampled_at` 决定当前结论，采样更新的合格样本成为片区当前版本，更早的漏送结果只留档（`supersedes`）。
- `POST /sampling/scope`（coordinator/dispatcher）：恢复前变更区域范围，当前轮所有样本标记作废、轮次 +1、清空队列并重新取样；旧样本结果不能再入账。
- `GET /sampling/pending`（dispatcher/regulator/coordinator）：列出已达标但当前样本版本未审批的片区。
- `POST /sampling/approve`（coordinator/regulator）：审批只对当前样本版本生效（可用 `expected_sample_id` 锁定），通过后固化浓度/时间快照；新采样版本出现后旧审批只留档、片区重回待审批。全部片区当前版本达标且审批后，`restore` 才放行。

内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。

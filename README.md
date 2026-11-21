# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验延迟分位数

`latency-register` 记录已经整文件封验的 JSONL 延迟数据来源；`latency-evaluate` 用最近秩定义计算精确 p95 并判定上限 SLA。该路线将全部数值与样本 ID 放入内存，默认最多 1000 行，超限时以 `latency_memory_limit` 拒绝。`fixtures/latency/` 和 `tests/test_latency.py` 提供可离线运行的合成样例。

`latency-assess` 是同一精确定义（最近秩 `ceil(0.95×N)`）的可中断恢复路线，面向内存与暂存双预算受限的环境：整文件 SHA-256 分块校验，JSONL 逐行严格解析（非负整数 `latency_ms`、非空且全批唯一 `sample_id`、UTF-8/JSONL 完整性），任一时刻内存中最多 `memory_rows` 个样本值和有界 ID 检查缓存，超出部分分批排序写入有界暂存（`scratch_bytes`），多路合并求全局精确秩，ID 经磁盘排序合并验证唯一。重复 ID、坏行、封验后字节漂移或零样本只给 `invalid` 且永不签发 SLA；预算确实不足时给 `indeterminate` 并附准确的资源缺口，不用部分顺序统计量冒充精确 p95。评估身份、冻结版本与文件摘要、`limit_ms`、总行数、精确秩和分阶段检查点持久化在 `latency_assessment`；同 ID 同内容重入从稳定检查点恢复或直接返回已签发结果，异文冲突（`assessment_identity_conflict`）。中断的运行不暴露半成品结论；恢复时重验源文件摘要与暂存段，漂移、段缺失或摘要不符即废弃重算。签发结果携带取秩边界证据（`count_less < rank ≤ count_le` 及文件摘要），`latency-recheck` 离线复核该值确为全局第 `ceil(0.95×N)` 个，`latency-assessment` 查询历次结论；新交付版本另起评估，旧结论仍按旧证据可查。

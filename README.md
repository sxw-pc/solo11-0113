# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验变更事务清单

`changefeed-register` 从已封验的 JSONL 冻结事务身份、来源接收序号、显式因果父事务与逐行 put/delete；`changefeed-inspect` 清点事务和缺行事务，但不重放实体状态。`fixtures/changefeed/` 是离线合成事件，可通过 `tests/test_changefeed.py` 运行。

## 实体键值逻辑快照

`changefeed-materialize --run RUN --policy {max_coverage,max_authority} --snapshot ID` 在同一冻结证据上构建接收方可查询的实体快照。缺行、重复 ordinal、缺因果父项或父图成环的事务不产生半个效果；入选集沿显式 parents 闭合，事务原子重放，并发键冲突（无祖先关系）不按到达顺序或 source_seq 擅自覆盖。两种 policy 是同一证据上的两条合法业务路线：`max_coverage` 先最大化事务数再最大化 authority，`max_authority` 反之，最终值可实质不同。求解为精确分支定界，预算耗尽返回 `indeterminate` 与已证界而不签发。`changefeed-verify` 从封验 JSONL 独立重放并核验最优目标、因果闭合和最终值；`changefeed-query` 只读查询。同 ID 同文幂等、异文冲突，修订另建 run/快照。27 笔事务、4 来源的多键冲突与依赖链见 `fixtures/changefeed_large/` 与 `tests/test_snapshot.py`（含与暴力枚举 oracle 的一致性核对）。

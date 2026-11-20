# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验实体数据清单

`entity-register` 从已封验的两个 JSONL 来源文件及候选关联边文件中冻结伪名化记录、来源内本地 ID 和正整数边权；`entity-inspect` 只报告原始记录量与候选边数，不推断跨来源是否为同一实体。`fixtures/entity-feed/` 和 `fixtures/entity-feed-spec.json` 是可用 `tests/test_entities.py` 离线运行的合成样例，不含真实个人信息。

## 实体对齐评估与证明

`entity-assess --policy operational|certified` 在冻结候选边上求解全局一对一最大权匹配：一条本地记录最多参与一条边，允许不匹配，先最大化全图 weight 总和，再在并列最优中考察匹配条数；去重实体数等于冻结原始行数减匹配条数。`operational` 在所有最高总分匹配中取匹配条数最多者，并按规范化边序贪心固定出一份确定性逐边方案，按所选去重人数对 threshold 给 pass/fail；`certified` 不任取一份，而是证明全部最高总分匹配的最少/最多条数，输出去重人数下上界及两端的可重放见证，仅当上界不超过 threshold 时 pass、下界超过时 fail，否则 indeterminate。多分量按连通分量分别求极值再合成，分量任取一份匹配不会被当作全集证明。搜索受 `--budget` 节点预算约束，预算耗尽只返回 indeterminate 与平凡上下界，绝不把一份可行匹配冒充最优。

评估结果（assessment_id、feed 版本、规范化行/边摘要、policy、threshold、最高总分、条数界、人数界、见证与结论）冻结在不可覆盖的 `entity_assessment` 台账：同 ID 同文幂等、异文冲突；新交付版本另建 feed 与 assessment，旧结果仍可复核且不追随新版本。`entity-verify` 从冻结行/边重算合法性、最优分数与两端极值界并回放见证，篡改边/行、漏掉并列最优、伪造界或换绑 feed 均不能通过。零权、重复边、未知端点在登记时拒绝；无边、同行竞争多边、多歧义分量与不匹配记录均有测试覆盖，另含 20+20 行稀疏图规模用例与预算耗尽用例。`fixtures/entity-feed-multi/` 是多分量合成样例。

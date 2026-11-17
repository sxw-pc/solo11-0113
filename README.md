# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验文档记录清单

`documents-register` 从已封验 JSONL 冻结文档 ID、修订号、标题与正文；`documents-inspect` 只清点文档与修订记录，不建立搜索索引。`fixtures/documents/` 与 `tests/test_documents.py` 是可离线运行的合成资料。

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

## 已封验文档短语检索

`documents-index --batch B --analyzer {exact|folded} --index ID` 从冻结文档建立持久倒排位置索引：每个 token 保存所属文档、修订、字段、字段内位置及原文最小连续 Unicode 码点区间，`title` 与 `body` 分别建位置，短语绝不跨字段拼接。

- `exact` 按 Unicode 空白分词，保留大小写与词内标点；`folded` 先 NFKC + casefold，再按非字母数字边界分词，因此连字符产生相邻词、大小写折叠。查询短语必须使用建索引时的同一分析器。
- `documents-search --index ID --phrase P [--max-revision N]` 只从索引读取。不指定上限时每文档仅取最高修订；指定正整数时每文档取不超过上限的最高修订，旧修订命中不会混入当前视图，历史视图也不因新修订无短语而丢失。
- 命中返回 `doc_id`、`revision`、`field`、原文起止码点偏移与原文片段。casefold 扩展（如 `ß`→`ss`）与兼容收缩（如全角字符）都映射回原文跨度，不用规范化偏移冒充原文偏移。
- 无分词或纯标点查询返回 `empty_phrase`；空短语（folded）或未匹配 token（exact）分别给确定错误或空结果。
- 索引记录绑定 batch/version/spec 摘要、行摘要、分析器规则版本与文档修订摘要；同一 ID 的异分析器/异批次请求抛 `index_identity_conflict`，相同请求幂等并返回同一修订摘要。构建在单事务内完成，中断后不暴露半索引；新交付版本另建索引，旧索引仍可按旧数据复核。
- `documents-verify --index ID --phrase P [--max-revision N] [--expected FILE]` 从冻结原文重做分词与位置，逐 token 核对持久记录，并比对指定命中与漏命中；篡改跨度、跨字段拼接或错用分析器都会抛错或给出 `false_positives` / `false_negatives`。

# 分段传验台设计

交付方冻结 `manifest`，其 `body_hash` 确定版本。`file_entry` 属于版本；`chunk` 属于文件，保存半开字节区间。每份移动介质回执产生 `attempt`，其中 `receipt` 连接块与实际哈希；`issue` 保留缺失、损坏、错版本证据。`decision` 只引用指定冻结版本。原始回执字节保存在 `attempt.source`。

批次状态为草拟、清单冻结、接收中、可发布、拒收或证据不足。块逐项核验后才可用于暂存重组；暂存文件写完用原子更名，封验再独立计算整文件大小与哈希。`unsafe_path`、`case_collision`、`unknown_version`、`source_changed`、`output_conflict` 会阻断相应命令；回执问题保留在 `issue` 中。清单和回执重入按摘要幂等，冻结版本不能修改。

`python -m chunkproof demo` 创建本地合成 10 字节交付、三个块、回执、暂存输出和 JSON/Markdown 证明。测试覆盖路径拒绝、空文件、块边界、回执重入与回滚、损坏候选、多介质选择、介质事后篡改、输出冲突和完整发布。

介质保管子系统用 `custody_event` 记录可信、隔离与交接顺序；收件验收子系统用 `recipient`、`acknowledgement`、`acceptance` 固化条件、回执和结论。验收同时读取封验、保管链、清单版本与回执，封验通过但保管证明失效时仍不能交付。

发布编排使用 `rollout`、`rollout_wave`、`rollout_member`、`rollout_decision` 固化波次、带权重的收件范围和验收快照。各波次顺序开放；权重门槛、必达收件人及实际争议共同决定等待、完成或中止。

文档检索子系统在 `document_row` 之上新增 `document_index`、`document_index_revision`、`document_token`：索引绑定批次、版本、spec 摘要、行摘要、分析器规则版本与修订摘要；每个 token 保存文档、修订、字段、字段内位置与原文最小连续码点跨度。`documents-index` 校验封装字节与批次摘要后单事务写入，`documents-search` 按字段内相邻位置匹配并按 `max_revision` 选择每文档可见修订，`documents-verify` 从冻结原文重放分词、核对每个持久 token 与命中/漏命中集合。

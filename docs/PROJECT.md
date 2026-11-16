# 分段传验台设计

交付方冻结 `manifest`，其 `body_hash` 确定版本。`file_entry` 属于版本；`chunk` 属于文件，保存半开字节区间。每份移动介质回执产生 `attempt`，其中 `receipt` 连接块与实际哈希；`issue` 保留缺失、损坏、错版本证据。`decision` 只引用指定冻结版本。原始回执字节保存在 `attempt.source`。

批次状态为草拟、清单冻结、接收中、可发布、拒收或证据不足。块逐项核验后才可用于暂存重组；暂存文件写完用原子更名，封验再独立计算整文件大小与哈希。`unsafe_path`、`case_collision`、`unknown_version`、`source_changed`、`output_conflict` 会阻断相应命令；回执问题保留在 `issue` 中。清单和回执重入按摘要幂等，冻结版本不能修改。

`python -m chunkproof demo` 创建本地合成 10 字节交付、三个块、回执、暂存输出和 JSON/Markdown 证明。测试覆盖路径拒绝、空文件、块边界、回执重入与回滚、损坏候选、多介质选择、介质事后篡改、输出冲突和完整发布。

介质保管子系统用 `custody_event` 记录可信、隔离与交接顺序；收件验收子系统用 `recipient`、`acknowledgement`、`acceptance` 固化条件、回执和结论。验收同时读取封验、保管链、清单版本与回执，封验通过但保管证明失效时仍不能交付。

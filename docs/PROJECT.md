# 分段传验台设计

交付方冻结 `manifest`，其 `body_hash` 确定版本。`file_entry` 属于版本；`chunk` 属于文件，保存半开字节区间。每份移动介质回执产生 `attempt`，其中 `receipt` 连接块与实际哈希；`issue` 保留缺失、损坏、错版本证据。`decision` 只引用指定冻结版本。原始回执字节保存在 `attempt.source`。

批次状态为草拟、清单冻结、接收中、可发布、拒收或证据不足。块逐项核验后才可用于暂存重组；暂存文件写完用原子更名，封验再独立计算整文件大小与哈希。`unsafe_path`、`case_collision`、`unknown_version`、`source_changed`、`output_conflict` 会阻断相应命令；回执问题保留在 `issue` 中。清单和回执重入按摘要幂等，冻结版本不能修改。

`python -m chunkproof demo` 创建本地合成 10 字节交付、三个块、回执、暂存输出和 JSON/Markdown 证明。测试覆盖路径拒绝、空文件、块边界、回执重入与回滚、损坏候选、多介质选择、介质事后篡改、输出冲突和完整发布。

介质保管子系统用 `custody_event` 记录可信、隔离与交接顺序；收件验收子系统用 `recipient`、`acknowledgement`、`acceptance` 固化条件、回执和结论。验收同时读取封验、保管链、清单版本与回执，封验通过但保管证明失效时仍不能交付。

发布编排使用 `rollout`、`rollout_wave`、`rollout_member`、`rollout_decision` 固化波次、带权重的收件范围和验收快照。各波次顺序开放；权重门槛、必达收件人及实际争议共同决定等待、完成或中止。

延迟分位数有两种同定义路线。`latency-evaluate` 是小批量全内存路线，超过 `max_rows` 即拒绝。`latency-assess` 把一次评估固化在 `latency_assessment`：身份指纹绑定批次、冻结版本、文件摘要、`limit_ms` 与双预算；计算分阶段（校验分块哈希与严格解析并溢出排序段、ID 归并查重、值归并取秩）并把稳定检查点（总行数、精确秩、暂存段摘要）持久化。内存任一时刻最多 `memory_rows` 个样本值与有界 ID 缓存，暂存写入逐字节计入 `scratch_bytes`。重入时同内容从检查点恢复或返回已签发结果，异文冲突；源漂移、暂存段缺失或摘要不符废弃该次计算；数据问题给 `invalid`，预算不足给 `indeterminate` 并附资源缺口，二者都不签发 SLA。签发结果保存 `count_less`/`count_le` 取秩边界证据，`latency-recheck` 可离线复核。

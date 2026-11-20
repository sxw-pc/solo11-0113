# 分段传验台设计

交付方冻结 `manifest`，其 `body_hash` 确定版本。`file_entry` 属于版本；`chunk` 属于文件，保存半开字节区间。每份移动介质回执产生 `attempt`，其中 `receipt` 连接块与实际哈希；`issue` 保留缺失、损坏、错版本证据。`decision` 只引用指定冻结版本。原始回执字节保存在 `attempt.source`。

批次状态为草拟、清单冻结、接收中、可发布、拒收或证据不足。块逐项核验后才可用于暂存重组；暂存文件写完用原子更名，封验再独立计算整文件大小与哈希。`unsafe_path`、`case_collision`、`unknown_version`、`source_changed`、`output_conflict` 会阻断相应命令；回执问题保留在 `issue` 中。清单和回执重入按摘要幂等，冻结版本不能修改。

`python -m chunkproof demo` 创建本地合成 10 字节交付、三个块、回执、暂存输出和 JSON/Markdown 证明。测试覆盖路径拒绝、空文件、块边界、回执重入与回滚、损坏候选、多介质选择、介质事后篡改、输出冲突和完整发布。

介质保管子系统用 `custody_event` 记录可信、隔离与交接顺序；收件验收子系统用 `recipient`、`acknowledgement`、`acceptance` 固化条件、回执和结论。验收同时读取封验、保管链、清单版本与回执，封验通过但保管证明失效时仍不能交付。

发布编排使用 `rollout`、`rollout_wave`、`rollout_member`、`rollout_decision` 固化波次、带权重的收件范围和验收快照。各波次顺序开放；权重门槛、必达收件人及实际争议共同决定等待、完成或中止。

变更数据子系统只从已封验版本的 JSONL 字节冻结证据。`changefeed-register` 登记事务（`tx_id`、来源 `source`/`source_seq`、显式 `parents`、`expected_rows`、`authority`）与逐行 `put`/`delete`，不重放；`changefeed-inspect` 只清点事务数、行数和缺行事务。`source_seq` 仅为传输序号，不代表因果先后。

`changefeed-materialize(run_id, policy, snapshot_id)` 在冻结证据上构建接收方可查询的实体键值快照。事务先经结构资格判定：ordinal 0..expected_rows-1 必须齐全且不重复（否则 `missing_ordinal`），显式父项须存在且自身合格（否则 `missing_parent`），父图成环（`causal_cycle`）及其下游一律排除；缺行、重复、缺父、成环的事务不产生半个效果。入选集沿显式 `parents` 向下闭合，事务内多行原子重放（put 写值、delete 移除键）。同一键上无祖先关系的两笔完整事务构成并发冲突，不按到达顺序、`source_seq` 或字典序覆盖；有祖先关系时按因果拓扑应用后代值，互不冲突的跨来源事务合并。

两种事先约定的合法目标在同一冻结证据上求解：`max_coverage` 先最大化事务数再最大化 authority 总和，`max_authority` 先最大化 authority 总和再最大化事务数，仍并列时以规范化 tx_id 集合稳定破同分。求解用精确分支定界（闭包固定点保证每个搜索状态可行，乐观上界剪枝，逐事务贪心或拆冲突行均无法冒充最优），节点预算耗尽只返回 `indeterminate` 与已证界，绝不签发快照。封存内容含 snapshot_id、冻结 run/version/spec、事务与行摘要、policy、候选与排除原因、两目标的全局最优证书、因果重放顺序、每个实体键值及最后写入事务。同 ID 同文幂等、异文冲突；新交付修订另建 run 与快照，旧快照不回写。`changefeed-verify` 从封验 JSONL 独立重建并重放，重证最优目标、因果闭合与最终值，篡改选择、丢行或改值均不能通过；`changefeed-query` 对已封快照提供只读键值查询。

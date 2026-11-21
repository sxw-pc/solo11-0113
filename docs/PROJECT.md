# 分段传验台设计

交付方冻结 `manifest`，其 `body_hash` 确定版本。`file_entry` 属于版本；`chunk` 属于文件，保存半开字节区间。每份移动介质回执产生 `attempt`，其中 `receipt` 连接块与实际哈希；`issue` 保留缺失、损坏、错版本证据。`decision` 只引用指定冻结版本。原始回执字节保存在 `attempt.source`。

批次状态为草拟、清单冻结、接收中、可发布、拒收或证据不足。块逐项核验后才可用于暂存重组；暂存文件写完用原子更名，封验再独立计算整文件大小与哈希。`unsafe_path`、`case_collision`、`unknown_version`、`source_changed`、`output_conflict` 会阻断相应命令；回执问题保留在 `issue` 中。清单和回执重入按摘要幂等，冻结版本不能修改。

`python -m chunkproof demo` 创建本地合成 10 字节交付、三个块、回执、暂存输出和 JSON/Markdown 证明。测试覆盖路径拒绝、空文件、块边界、回执重入与回滚、损坏候选、多介质选择、介质事后篡改、输出冲突和完整发布。

介质保管子系统用 `custody_event` 记录可信、隔离与交接顺序；收件验收子系统用 `recipient`、`acknowledgement`、`acceptance` 固化条件、回执和结论。验收同时读取封验、保管链、清单版本与回执，封验通过但保管证明失效时仍不能交付。

发布编排使用 `rollout`、`rollout_wave`、`rollout_member`、`rollout_decision` 固化波次、带权重的收件范围和验收快照。各波次顺序开放；权重门槛、必达收件人及实际争议共同决定等待、完成或中止。

## 遥测时窗裁定

`telemetry_run/sample/anchor` 冻结一次运行的设备读数、独立 seq、逻辑 tick、非负 value 与校准锚点；`telemetry_assessment` 保存不可覆盖的时窗裁定证书。每台设备共享一组仿射时钟参数 `utc_ms = offset + rate×tick`，锚点以半平面 `utc_lo ≤ offset + rate×tick ≤ utc_hi` 约束，rate 限制在规格的整数端点之间连续取值。

`clockmath` 用 Fraction 精确算术把可行参数集合构造成凸多边形（Sutherland–Hodgman 裁剪），再用读数的两条成员关系直线 `offset + rate×tick = start/end` 与锚点直线、rate 边界组成直线排列。可行多边形按这些直线划分为 0D 顶点、1D 线段和 2D 单元，窗内总量在每个单元内部恒定；枚举顶点、可行线段中点及线段两侧严格内点即覆盖全部单元，保证 guaranteed 合同的下上界是全局精确界而非网格猜测。退化区域（单点、单线段、平行重叠边）单独处理，预算耗尽时只返回已证见证并标记非精确。

`nominal` 合同先验证提交时钟满足 rate 区间和全部锚点，再逐读数按半开区间 `[start,end)` 归属；`guaranteed` 合同输出精确界、达成界的可重放时钟和落窗 tick 见证，按下界≥门槛、上界<门槛或跨门槛给出 pass/fail/indeterminate，校准则无解时给出最小冲突锚点引用。证书按规范化 JSON 的 sha256 实现同 ID 同文幂等、异文冲突；`telemetry-verify` 重新读取封验字节哈希并从冻结证据重算全部归属、见证可行性与最终裁定。

# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验遥测证据

`telemetry-register` 从已封验的 JSONL 文件冻结设备读数、逻辑 tick、非负量值和每设备校准锚点；`telemetry-inspect` 只清点设备读数、原始总量和锚点，不推断读数属于哪个 UTC 时窗。`fixtures/telemetry-run/` 给出本地合成读数与时钟证据，可通过 `tests/test_telemetry.py` 运行。

## 遥测时窗裁定

`telemetry-assess(run_id, window_start_ms, window_end_ms, threshold, policy, assessment_id)` 在不改变块补齐、介质磨损、收件/波次或其他逻辑实体去重结论的前提下，裁定接收方在半开 UTC 时窗 `[start,end)` 内的业务量是否达到门槛。每台设备的一次运行只有一组共享仿射时钟 `utc_ms = offset_ms + rate_ms_per_tick × tick`，rate 为规格允许范围内的实数，每个校准锚点都要求 `utc_lo_ms ≤ offset + rate×tick ≤ utc_hi_ms`；同设备全部读数与锚点共用同一组参数，绝不按读数各自挑选有利端点。

两种分析合同在查询前显式选定：

- `nominal`：提交每台设备的指定 offset/rate，先验证其同时满足全部锚点和 rate 区间，再按该时钟逐读数归属并汇总；参数随证书留存，不能只采用有利时钟而不记录。锚点不满足时结论为 `invalid` 并列出冲突。
- `guaranteed`：在全部物理可行的共享时钟参数上求窗内总量的精确全局下上界（Fraction 精确有理数，按关键事件直线对可行多边形做排列划分，而非采样网格），并为每个界提供可重放的共享时钟与落窗读数见证。下界 ≥ 门槛为 `pass`，上界 < 门槛为 `fail`，其余 `indeterminate`；无可行校准时为 `invalid` 并给出最小冲突锚点引用；预算耗尽时为 `indeterminate`，只保留已证见证界，不冒充全局界。

多设备参数相互独立，分别求界后合成，同设备样本不拆分。半开端点相等、可行区域退化为线段或点、漂移改变读数归属、矛盾锚点、非法窗口/门槛都有确定结论。评估证书（assessment_id、冻结 version/spec 摘要、全部读数/锚点摘要、窗口、门槛、合同、名义时钟或界见证）以内容哈希存入不可覆盖台账：同 ID 同文幂等、异文冲突；新交付版本另建 run，旧证书仍按旧冻结证据复核。`telemetry-verify` 从冻结字节、锚点和所报参数重新推导名义归属或保证界，篡改锚点/封验字节、伪造见证、漏掉边界可行解释或换 run 都不能通过。

`fixtures/telemetry-multi/` 提供 3 台设备、45 条读数、8 个锚点和连续 rate 区间的合成数据；`tests/test_telemetry_assess.py` 覆盖两合同分歧、共享偏移与漂移/半开端点、多设备与可行区域退化、无解/预算/证书，以及版本、幂等、复核与不回归五组验收。

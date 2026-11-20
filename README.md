# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验地形格网与出口径流核算

`terrain-register` 从已封验 JSONL 冻结整数坐标/高程、非负本地径流和出口；`terrain-inspect` 只清点格点、原始径流与出口数，不做任何流向或汇水计算。`fixtures/terrain-branch/` 与 `fixtures/terrain-flat/` 是 `tests/test_terrain.py` 可离线运行的合成资料。

`terrain-route` 在已登记格网上做离线汇流核算，要求调用者先声明一种合法模型：

- `single_flow`：每个非终端平坦区把全部水量沿高差最大的单条边送走，平手按 `(y,x,下游区代表坐标)` 稳定破同分，与输入行序无关。
- `multiple_flow`：按所有严格下降边的权重比例分水，同一接收区的多条边先合并权重，全程用精确有理数，不做逐格舍入。

相同高程且四邻接连通的格点先凝聚为平坦区，区内的本地径流合并后再统一判定外流；只能流向严格更低的邻区，因此按高程排序即为无环传播。出口所在区必须是局部最低且不含第二个出口；无更低邻区的封闭洼地保留水量并记录 `closed_depression`，绝不按最近出口猜测。缺测坐标保持缺失，不补造格点。

每个核算结果保存 `route_id`、冻结的 grid/version/格点摘要、模型、阈值、逐区流向与分流分子/分母、逐出口上游贡献、保留量及守恒证据。同 ID 同文幂等，异文返回 `terrain_route_identity_conflict`；新交付版本另建 route，旧结果仍按旧数据复核。预算耗尽时结果为 `indeterminate`，不签发任何出口负荷。`terrain-verify` 从冻结格点重算分区、分流、出口/保留量和阈值结论，任一格点或证据被篡改都会失败。

同一份观测下两种模型可以给出不同的业务结论：`fixtures/terrain-branch/` 中 `single_flow` 得 `L=3/R=1`（L 阈值 3 通过），`multiple_flow` 得 `L=2/R=2`（阈值 3 未通过）；`fixtures/terrain-flat/` 中三格同高区分别得 `L=4/R=1` 与 `L=5/2、R=5/2`。


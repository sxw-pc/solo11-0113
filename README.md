# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

## 交付证明与冲突处理

全文件哈希吻合仍不足以放行：必须有组装记录，且组装记录引用本批冻结清单下每个已验真块的回执。回执自报哈希与实际字节摘要矛盾时，候选标为 `claim_conflict`，不会被选中；另一个介质的有效副本仍可完成交付。`seal` 对无证明但内容恰好相同的目标返回“证据不足”。

## 介质保管与收件验收

`custody-transfer` 记录介质交接链，`recipient` 冻结收件要求，`acknowledge` 记录回执，`acceptance` 作最终验收。交接链断裂或隔离介质会使已封验文件仍待核；清单摘要不一致、回执冲突或超期分别产生不同验收结论。相关记录均保存在 SQLite 台账，`demo` 覆盖完整交付。

`rollout-create` 将收件人分入不可变的发布波次，指定权重、通过门槛和必达收件人。`rollout-evaluate` 从实际验收记录计算每波次能否开放、等待、中止或完成；试点未完成时后续波次不开放，必达收件人争议会中止发布。逐收件结论保存在 `rollout_decision`。

## 已封验结构化记录帧

`frame-register` 冻结已封验的 CPR1 帧文件；`frame-decode` 对小文件按 magic、长度、序号、CRC 和 JSON 事件 ID 严格解析，遇到任何损坏即停止。`fixtures/framed-events.json` 定义可在 `tests/test_framed.py` 本地生成的正常与中间损坏样例；文件字节封验通过并不代表内部每帧可解码。

## 有界流式记录级导入

`frame-import --mode complete_stream|independent_records` 在读字节前先冻结使用合同，并以有界内存的前向流式扫描处理可远大于内存的文件：驻留量只与读取块和单个重同步窗口有关，单帧 payload 不超过 `MAX_PAYLOAD`，不为每个偏移复制剩余文件，因此时间和内存都是线性有界的。

- `complete_stream` 要求从序号 1 起连续，且每帧头、声明长度、CRC、UTF-8、JSON、事件身份与序号全部有效；任何损坏都拒收整次导入，不产生可用事件。
- `independent_records` 保留损坏前后可独立端到端验证的完整帧，输出缺口字节区间与序号缺口；坏帧永远不会被改写成好事件。

重同步区分三类证据：帧头与声明长度仍可解析但 CRC 坏时，声明长度只作为候选跳跃，后续帧仍独立验证；帧头也坏时只对之后字节做有界候选扫描；候选起点必须通过完整边界、CRC、UTF-8、JSON 与序号检查，payload 内出现的 `CPR1`（甚至看似可解的嵌套帧）本身不构成边界。同一坏区间存在多个互斥且各自完整的重同步解释时给出 `ambiguous_resync` 并隔离不确定后缀，而不是选择最早或最新的伪帧；有界扫描预算耗尽或扫描中断时结论为 `indeterminate`，绝不输出半完成的成功导入。

`import_id`、模式、冻结文件摘要、每个已验证帧的 offset/length/sequence/event_id/帧摘要、issue 原始字节区间和最终结论写入只增不改的台账（`frame_import`、`frame_import_event`、`frame_import_issue`）。完整成功或独立恢复的可用事件在同一事务提交；同 `import_id` 同文幂等，异文或模式冲突报错；源文件事后被改、扫描中断或重同步仍歧义时不能沿用旧成功结果。`frame-verify` 从冻结来源字节逐段重新推导并复核帧位置、CRC、缺口与可用事件集合，手改 issue 范围或事件行、或凭整文件封验跳过帧验证都会被拒绝。`fixtures/framed-damage.json` 与 `tests/test_frame_import.py` 离线合成坏头重同步、伪 magic、歧义重同步、大文件与中断等样例。

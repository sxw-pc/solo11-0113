# 分段传验台

移动介质大文件交付的分块验真与安全重组。流程为冻结清单、块计划、回执、逐块验真、候选裁定、原子重组、全文件封验和证据报告。失败与冲突保留，不将缺证据判作可发布。

运行：`python -m pip install . --no-deps --no-build-isolation`，`python -m chunkproof demo`，`python -m chunkproof --help`，`python -m pytest -q`。运行与测试使用本地合成数据，不访问网络。

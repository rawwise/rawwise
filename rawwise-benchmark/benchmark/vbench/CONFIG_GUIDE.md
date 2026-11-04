# VBench 视频生成 API 配置指南

本文档帮你配置真实视频生成 API（CogVideoX / Kling / VIDU），跑 RawWise 端到端对照实验。

---

## 配置入口（3 种方式，按优先级）

| 优先级 | 方式 | 用途 |
|-------|------|------|
| 1 (最高) | Java System Property | CI/CD 一次性运行 |
| 2 | 环境变量 | 临时测试 |
| 3 | 配置文件 `~/.rawwise/vbench/config.properties` | 持久化（推荐） |
| 4 (兜底) | 代码默认值 | 无配置时用 stub |

---

## 推荐：用配置文件持久化

### 步骤 1：创建配置文件

```bash
mkdir -p ~/.rawwise/vbench
cp rawwise-benchmark/benchmark/vbench/config.properties.template \
   ~/.rawwise/vbench/config.properties
```

### 步骤 2：编辑并填入真实值

```bash
vim ~/.rawwise/vbench/config.properties
```

文件内容示例（以智谱 CogVideoX 为例）：

```properties
provider=cogvideox-api
provider.api.endpoint=https://open.bigmodel.cn/api/paas/v4/videos/generations
provider.api.key=你的真实API_KEY
provider.api.model=cogvideox-2
provider.timeout.seconds=300

videos.output.dir=benchmark/vbench/videos
eval.results.dir=benchmark/vbench/eval_results
eval.python.path=rawwise-benchmark/benchmark/vbench/.venv/bin/python
eval.script.path=rawwise-benchmark/benchmark/vbench/clip_score_eval.py
eval.hf.endpoint=https://hf-mirror.com
```

### 步骤 3：验证配置生效

```bash
# 在仓库根目录执行
mvn exec:java -pl rawwise-benchmark \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.args="--print-config" \
    -Dexec.classpathScope=compile -q
```

期望输出（API key 应该被脱敏显示）：

```
=== VBench Configuration ===
  provider                         = cogvideox-api  (file)
  provider.api.endpoint            = https://open.bigmodel.cn/...  (file)
  provider.api.key                 = sk-x***xxxx  (file)         ← 已脱敏
  provider.api.model               = cogvideox-2  (file)
  ...
```

---

## 各 Provider 配置参数

### 智谱 CogVideoX（推荐）

| 字段 | 值 |
|------|-----|
| `provider` | `cogvideox-api` |
| `provider.api.endpoint` | `https://open.bigmodel.cn/api/paas/v4/videos/generations` |
| `provider.api.model` | `cogvideox-2` (或 `cogvideox-flash`) |
| API key 获取 | https://bigmodel.cn → 控制台 → API Keys |
| 单视频成本 | ~¥0.5-2 RMB |
| 视频时长 | 5-10 秒 |
| 分辨率 | 720x480 / 1024x768 |

### 可灵 Kling

| 字段 | 值 |
|------|-----|
| `provider` | `kling` |
| `provider.api.endpoint` | `https://api.klingai.com/v1/videos/text2video` |
| `provider.api.model` | `kling-v1` (或 `kling-v1.5`) |
| API key 获取 | https://klingai.com → 开放平台 |
| 单视频成本 | ~¥1-5 RMB |
| 视频时长 | 5/10 秒 |

### VIDU

| 字段 | 值 |
|------|-----|
| `provider` | `vidu` |
| `provider.api.endpoint` | `https://api.vidu.studio/ent/v2/text2video` |
| `provider.api.model` | `vidu-1.5` |
| API key 获取 | https://vidu.studio → API |
| 单视频成本 | ~¥1-3 RMB |

### 阿里通义 Wan

| 字段 | 值 |
|------|-----|
| `provider` | `wan` |
| `provider.api.endpoint` | `https://dashscope.aliyuncs.com/api/v1/services/aigc/video-generation/video-synthesis` |
| `provider.api.model` | `wan-2.1-t2v` |
| API key 获取 | https://dashscope.console.aliyun.com |

---

## 替代方式 1：环境变量（临时测试）

```bash
export RAWWISE_VBENCH_PROVIDER=cogvideox-api
export RAWWISE_VBENCH_PROVIDER_API_KEY=你的key
export RAWWISE_VBENCH_PROVIDER_API_ENDPOINT=https://open.bigmodel.cn/api/paas/v4/videos/generations
export RAWWISE_VBENCH_PROVIDER_API_MODEL=cogvideox-2

mvn exec:java -pl rawwise-benchmark \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.classpathScope=compile
```

**命名规则**：所有 key 加前缀 `RAWWISE_VBENCH_`，点 (`.`) 替换为下划线 (`_`)，全大写。

| 配置 key | 环境变量 |
|---------|---------|
| `provider` | `RAWWISE_VBENCH_PROVIDER` |
| `provider.api.endpoint` | `RAWWISE_VBENCH_PROVIDER_API_ENDPOINT` |
| `provider.api.key` | `RAWWISE_VBENCH_PROVIDER_API_KEY` |
| `provider.api.model` | `RAWWISE_VBENCH_PROVIDER_API_MODEL` |
| `provider.timeout.seconds` | `RAWWISE_VBENCH_PROVIDER_TIMEOUT_SECONDS` |
| `videos.output.dir` | `RAWWISE_VBENCH_VIDEOS_OUTPUT_DIR` |

---

## 替代方式 2：Java System Property（CI/CD）

```bash
mvn exec:java -pl rawwise-benchmark \
    -Drawwise.vbench.provider=cogvideox-api \
    -Drawwise.vbench.provider.api.key=你的key \
    -Drawwise.vbench.provider.api.endpoint=https://open.bigmodel.cn/api/paas/v4/videos/generations \
    -Drawwise.vbench.provider.api.model=cogvideox-2 \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.classpathScope=compile
```

---

## 完整运行流程

### 步骤 1：配置 API key（按上面任一方式）

### 步骤 2：仅打印配置确认无误

```bash
mvn exec:java -pl rawwise-benchmark \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.args="--print-config" \
    -Dexec.classpathScope=compile -q
```

确认 `provider.api.key` 来源是 `(file)` 或 `(env)`，不是 `(default)`。

### 步骤 3：跑真实视频生成 + VBench 评测

```bash
mvn exec:java -pl rawwise-benchmark \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.classpathScope=compile -q
```

### 步骤 4：查看视频产物

```bash
ls -lh benchmark/vbench/videos/baseline/
ls -lh benchmark/vbench/videos/rawwise/

# 视频文件
benchmark/vbench/videos/baseline/prompt_001_run_0.mp4
benchmark/vbench/videos/baseline/prompt_001_run_0.meta.json

# meta.json 含 prompt / API 响应 / 生成耗时
cat benchmark/vbench/videos/baseline/prompt_001_run_0.meta.json
```

### 步骤 5：跑 VBench-Subset 评测对比

```bash
cd rawwise-benchmark/benchmark/vbench

# Baseline 视频评测
.venv/bin/python vbench_mps_eval.py \
    --video videos/baseline/prompt_001_run_0.mp4 \
    --prompt "A young woman in a red jacket walking down a street" \
    --num-frames 8

# RawWise 视频评测
.venv/bin/python vbench_mps_eval.py \
    --video videos/rawwise/prompt_001_run_0.mp4 \
    --prompt "A young woman in a red jacket walking down a street" \
    --num-frames 8
```

---

## 视频产物存储

所有视频和评测结果都写入磁盘：

```
benchmark/vbench/videos/
├── baseline/                          ← 无 RawWise 的视频
│   ├── prompt_001_run_0.mp4
│   ├── prompt_001_run_0.meta.json
│   ├── prompt_002_run_0.mp4
│   └── prompt_002_run_0.meta.json
├── rawwise/                           ← 有 RawWise 辅助的视频
│   ├── prompt_001_run_0.mp4
│   └── prompt_001_run_0.meta.json
└── corrections/                       ← 反馈闭环重生成的视频

benchmark/vbench/eval_results/         ← 评测结果 JSON
├── baseline_vbench.json
└── rawwise_vbench.json
```

**所有这些目录已加入 `.gitignore`，不会进版本控制。**

---

## meta.json 结构

```json
{
  "prompt_id": "prompt_001",
  "prompt": "A young woman in a red jacket walking down a street",
  "tag": "baseline",
  "run_id": 0,
  "video_path": "benchmark/vbench/videos/baseline/prompt_001_run_0.mp4",
  "file_size_bytes": 1234567,
  "model_version": "cogvideox-2",
  "elapsed_ms": 45230,
  "video_url": "https://cogvideo.cdn.bigmodel.cn/xxx.mp4",
  "has_reference_embedding": false
}
```

---

## 成本预估

| 实验规模 | 视频数 | 成本（智谱 CogVideoX） | 时间 |
|---------|-------|----------------------|------|
| 冒烟测试 | 2 prompts × 2 组 = 4 个视频 | ~¥4-8 | 5-10 分钟 |
| 小型对照实验 | 10 prompts × 2 组 = 20 个视频 | ~¥20-40 | 30-60 分钟 |
| 完整 VBench-mini | 50 prompts × 2 组 = 100 个视频 | ~¥100-200 | 4-8 小时 |

---

## 安全提示

1. **配置文件已 gitignore**：`~/.rawwise/vbench/config.properties` 不会进版本控制
2. **API key 不打印明文**：日志中显示为 `sk-x***xxxx`（前 4 + 后 4 字符）
3. **不要 commit 真实 key**：避免在 `git diff` 时不小心提交

---

## 常见问题

**Q：配置完没生效？**

A：用 `--print-config` 模式确认。`provider.api.key` 的来源应是 `(env)` 或 `(file)`，不是 `(default)`。

**Q：HTTP 403 / 401？**

A：API key 错误，或账户未充值。去 provider 控制台检查余额。

**Q：视频下载超时？**

A：调大 `provider.timeout.seconds=600`。

**Q：生成的视频 prompt 没遵循？**

A：尝试英文 prompt，部分模型对中文支持差。

**Q：想换不同 provider？**

A：改 `provider.api.endpoint` + `provider.api.key` + `provider.api.model` 三个值即可。代码逻辑自适配 OpenAI 风格协议。

**Q：自定义 prompt 列表？**

A：修改 [RealVideoBenchmarkRunner.java](../../src/main/java/io/github/rawwise/benchmark/vbench/RealVideoBenchmarkRunner.java) 的 `List<String> prompts` 或扩展 [VideoPromptDataset.java](../../src/main/java/io/github/rawwise/benchmark/vbench/VideoPromptDataset.java)。

---

## 配置完成 checklist

- [ ] `~/.rawwise/vbench/config.properties` 已创建
- [ ] `provider.api.key` 已填入真实 API key
- [ ] `provider.api.endpoint` 已填入真实 URL
- [ ] `provider.api.model` 已填入模型名
- [ ] `--print-config` 验证配置生效（key 来源是 file/env）
- [ ] Provider 平台账户有余额
- [ ] 准备 ¥20-200 实验预算
- [ ] 准备 1-8 小时实验时间

配置完后，直接跑：

```bash
# 在仓库根目录执行
mvn exec:java -pl rawwise-benchmark \
    -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
    -Dexec.classpathScope=compile -q
```

会自动：
1. 调用真实 API 生成 2 个 baseline 视频 + 2 个 rawwise 视频
2. 保存到 `benchmark/vbench/videos/`
3. 写入元数据 .meta.json

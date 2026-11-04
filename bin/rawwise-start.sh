#!/bin/bash
# RawWise 本地部署启动脚本 — 单进程 Embedded 模式 (Frontend + Backend + Executor + Metastore 同 JVM)
# 用法: ./rawwise-start.sh [--profile=dogfood|benchmark|test] [--foreground]
#   --profile=<name>   选择部署 profile（默认 dogfood）
#                        dogfood   : 端口 8080/9001, 数据 ~/.rawwise/data/      （常驻生产）
#                        benchmark : 端口 8081/9002, 数据 /tmp/rawwise-bench/    （一次性压测）
#                        test      : 端口 8082/9003, 数据 /tmp/rawwise-test/    （临时验证）
#   --foreground       前台运行，Ctrl+C 停止
#
# 数据持久化在各自 profile 的 data 目录，重启不会丢失
# 如需清空数据: bash bin/rawwise-clean.sh --profile=<name>
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RAW_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ====== Profile 解析 ======
PROFILE="${RAWWISE_PROFILE:-dogfood}"
for arg in "$@"; do
    case "$arg" in
        --profile=*) PROFILE="${arg#*=}" ;;
        --profile)   shift; PROFILE="${1:-}" ;;
    esac
done

case "$PROFILE" in
    dogfood)
        FRONTEND_PORT="${RAWWISE_FRONTEND_PORT:-8080}"
        METASTORE_PORT="${RAWWISE_METASTORE_PORT:-9001}"
        DATA_DIR="${RAWWISE_DATA_DIR:-$HOME/.rawwise/data}"
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        LOG_SUFFIX="$PROFILE"
        PROFILE_DESC="常驻生产 dogfooding"
        ;;
    benchmark)
        FRONTEND_PORT="${RAWWISE_FRONTEND_PORT:-8081}"
        METASTORE_PORT="${RAWWISE_METASTORE_PORT:-9002}"
        DATA_DIR="${RAWWISE_DATA_DIR:-/tmp/rawwise-bench}"
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        LOG_SUFFIX="$PROFILE"
        PROFILE_DESC="一次性压测 benchmark"
        ;;
    test)
        FRONTEND_PORT="${RAWWISE_FRONTEND_PORT:-8082}"
        METASTORE_PORT="${RAWWISE_METASTORE_PORT:-9003}"
        DATA_DIR="${RAWWISE_DATA_DIR:-/tmp/rawwise-test}"
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        LOG_SUFFIX="$PROFILE"
        PROFILE_DESC="临时验证 test"
        ;;
    *)
        echo "ERROR: Unknown profile: $PROFILE (use dogfood|benchmark|test)"
        exit 1
        ;;
esac

# ====== 加载 .env 凭证 ======
if [ -f "$RAW_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$RAW_ROOT/.env"
    set +a
    echo "==> Loaded .env"
fi
RAWWISE_EMBEDDING_TYPE="${RAWWISE_EMBEDDING_TYPE:-noop}"
RAWWISE_EMBEDDING_ENDPOINT="${RAWWISE_EMBEDDING_ENDPOINT:-}"
RAWWISE_EMBEDDING_MODEL="${RAWWISE_EMBEDDING_MODEL:-}"
RAWWISE_EMBEDDING_DIMENSION="${RAWWISE_EMBEDDING_DIMENSION:-1024}"
RAWWISE_EMBEDDING_API_KEY="${RAWWISE_EMBEDDING_API_KEY:-}"
RAWWISE_EMBEDDING_TIMEOUT_MS="${RAWWISE_EMBEDDING_TIMEOUT_MS:-30000}"
RAWWISE_EMBEDDING_MAX_RETRIES="${RAWWISE_EMBEDDING_MAX_RETRIES:-3}"
RAWWISE_EMBEDDING_BATCH_SIZE="${RAWWISE_EMBEDDING_BATCH_SIZE:-32}"
RAWWISE_EMBEDDING_FORMAT="${RAWWISE_EMBEDDING_FORMAT:-openai_standard}"
RAWWISE_AXIOM_DERIVATION_ASYNC_CONCURRENCY="${RAWWISE_AXIOM_DERIVATION_ASYNC_CONCURRENCY:-8}"
RAWWISE_SUMMARIZATION_TYPE="${RAWWISE_SUMMARIZATION_TYPE:-noop}"
RAWWISE_SUMMARIZATION_ENDPOINT="${RAWWISE_SUMMARIZATION_ENDPOINT:-}"
RAWWISE_SUMMARIZATION_MODEL="${RAWWISE_SUMMARIZATION_MODEL:-}"
RAWWISE_SUMMARIZATION_API_KEY="${RAWWISE_SUMMARIZATION_API_KEY:-}"
RAWWISE_RATE_LIMIT_PER_IP="${RAWWISE_RATE_LIMIT_PER_IP:-10000}"
RAWWISE_RATE_LIMIT_GLOBAL="${RAWWISE_RATE_LIMIT_GLOBAL:-50000}"
RAWWISE_FACT_CONCURRENCY="${RAWWISE_FACT_CONCURRENCY:-4}"
echo "    Profile:  $PROFILE ($PROFILE_DESC)"
echo "    Embedding type=$RAWWISE_EMBEDDING_TYPE  model=${RAWWISE_EMBEDDING_MODEL:-<unset>}  endpoint=${RAWWISE_EMBEDDING_ENDPOINT:-<unset>}  format=$RAWWISE_EMBEDDING_FORMAT"
echo "    Summarization type=$RAWWISE_SUMMARIZATION_TYPE  model=${RAWWISE_SUMMARIZATION_MODEL:-<unset>}"
echo "    Rate-limit per-ip=$RAWWISE_RATE_LIMIT_PER_IP  global=$RAWWISE_RATE_LIMIT_GLOBAL"

# ====== 配置 (可通过环境变量覆盖) ======
WAREHOUSE_PATH="${RAWWISE_WAREHOUSE_PATH:-$DATA_DIR/warehouse}"
METASTORE_PATH="${RAWWISE_METASTORE_PATH:-$DATA_DIR/metastore}"
TOKEN_DIR="${RAWWISE_TOKEN_DIR:-$DATA_DIR/tokens}"
EMBEDDED_HEAP="${RAWWISE_EMBEDDED_HEAP:-6g}"
MAX_DIRECT_MEMORY="${RAWWISE_MAX_DIRECT_MEMORY:-2g}"
STORAGE_LMDB_INIT_MB="${RAWWISE_STORAGE_LMDB_INIT_MB:-256}"
STORAGE_LMDB_MAX_MB="${RAWWISE_STORAGE_LMDB_MAX_MB:-4096}"
LOG_DIR="$RAW_ROOT/logs"
RUN_DIR="$(dirname "$PID_FILE")"
mkdir -p "$LOG_DIR" "$WAREHOUSE_PATH" "$METASTORE_PATH" "$TOKEN_DIR" "$RUN_DIR"

# ====== 残留进程检查（拒绝并发启动同一 profile） ======
if [ -f "$PID_FILE" ]; then
    EXISTING_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
    if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "ERROR: RawWise profile '$PROFILE' already running (PID $EXISTING_PID)"
        echo "       Stop it first: bash bin/rawwise-stop.sh --profile=$PROFILE"
        echo "       Or check status: bash bin/rawwise-status.sh --profile=$PROFILE"
        exit 1
    else
        echo "==> Stale PID file detected, removing: $PID_FILE"
        rm -f "$PID_FILE"
    fi
fi

# 兜底：即使 PID 文件不存在，也检查端口是否被占用（仅 LISTEN，避免瞬时出站连接误报）
PORT_PID=$(lsof -ti:"$FRONTEND_PORT" -sTCP:LISTEN 2>/dev/null || true)
if [ -n "$PORT_PID" ]; then
    echo "ERROR: Port $FRONTEND_PORT already occupied by PID $PORT_PID"
    echo "       This port belongs to profile '$PROFILE'. Stop the occupant first."
    exit 1
fi

# ====== 日志保留 ======
LOG_TS=$(date +%Y%m%d-%H%M%S)
RAW_LOG="$LOG_DIR/rawwise-$LOG_SUFFIX-$LOG_TS.log"
ln -sf "rawwise-$LOG_SUFFIX-$LOG_TS.log" "$LOG_DIR/rawwise-$LOG_SUFFIX.log"
ls -t "$LOG_DIR"/rawwise-$LOG_SUFFIX-*.log 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null

export RAWWISE_WAREHOUSE_PATH="$WAREHOUSE_PATH"

echo "==> Data directory: $DATA_DIR"
echo "    (persists across restarts; use bin/rawwise-clean.sh --profile=$PROFILE to reset)"
echo "==> Mode: Embedded (InProcess gRPC — single JVM, ports: HTTP :$FRONTEND_PORT + Metastore :$METASTORE_PORT)"
echo "    Heap: $EMBEDDED_HEAP  DirectMemory: $MAX_DIRECT_MEMORY"

# ====== Classpath ======
RAW_JAR_NAME="rawwise-core-0.0.1.jar"
POM_FILE="$RAW_ROOT/rawwise-core/pom.xml"
if [ -f "$POM_FILE" ]; then
    POM_VER=$(grep -m1 '<version>' "$POM_FILE" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
    [ -n "$POM_VER" ] && RAW_JAR_NAME="rawwise-core-${POM_VER}.jar"
fi

if [ -f "$RAW_ROOT/lib/classpath.txt" ]; then
    CP="${RAW_ROOT}/lib/${RAW_JAR_NAME}:$(cat "$RAW_ROOT/lib/classpath.txt")"
elif [ -f "/tmp/rawwise/cp-full.txt" ]; then
    CP="rawwise-core/target/classes:$(cat /tmp/rawwise/cp-full.txt)"
elif [ -f "$RAW_ROOT/rawwise-core/target/classes" ]; then
    echo "==> lib/classpath.txt not found, auto-computing classpath..."
    if mvn dependency:build-classpath -pl rawwise-core -Dmdep.outputFile=/tmp/rawwise/cp-full.txt -q -f "$RAW_ROOT/pom.xml" 2>/dev/null; then
        CP="rawwise-core/target/classes:$(cat /tmp/rawwise/cp-full.txt)"
        echo "==> Classpath computed and cached to /tmp/rawwise/cp-full.txt"
    else
        echo "ERROR: Failed to compute classpath. Run 'bash bin/rawwise-package.sh' first."
        exit 1
    fi
else
    echo "ERROR: classpath not found and project not compiled. Run 'bash bin/rawwise-package.sh' first."
    exit 1
fi

# ====== JVM 基础参数 ======
JVM_BASE="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -XX:+UseZGC -XX:+ZGenerational"

# ====== 进程别名 ======
JAVA_BIN=$(which java)
RAW_LINK="/tmp/rawwise"
if [ -d "$RAW_LINK" ] && [ ! -L "$RAW_LINK" ]; then
    RAW_LINK="$RAW_LINK/java"
elif [ -L "$RAW_LINK" ]; then
    rm -f "$RAW_LINK" 2>/dev/null || true
fi
ln -sf "$JAVA_BIN" "$RAW_LINK"

# ====== 自动重编译 JAR（每次启动前检查源码是否新于 JAR） ======
RAW_SRC_DIR="$RAW_ROOT/rawwise-core/src/main/java"
RAW_JAR="$RAW_ROOT/lib/${RAW_JAR_NAME}"
if [ -d "$RAW_SRC_DIR" ] && [ -f "$RAW_JAR" ]; then
    NEWEST_SRC=$(find "$RAW_SRC_DIR" -name "*.java" -newer "$RAW_JAR" 2>/dev/null | head -1)
    if [ -n "$NEWEST_SRC" ]; then
        echo "==> Source newer than JAR, recompiling..."
        if mvn package -pl rawwise-core -DskipTests -q -f "$RAW_ROOT/pom.xml" 2>/dev/null; then
            cp "$RAW_ROOT/rawwise-core/target/${RAW_JAR_NAME}" "$RAW_JAR"
            echo "==> JAR rebuilt and updated"
        else
            echo "WARN: mvn package failed, using existing JAR"
        fi
    fi
fi

# ====== 清理函数 ======
RAW_PID=""
cleanup() {
    echo ""
    echo "==> Stopping RawWise (profile=$PROFILE)..."
    [ -n "$RAW_PID" ] && kill "$RAW_PID" 2>/dev/null
    wait 2>/dev/null
    echo "==> Stopped. Data preserved in $DATA_DIR"
}
if [ "${1:-}" = "--foreground" ]; then
    trap cleanup EXIT INT TERM
fi

# ====== 启动 (单进程 Embedded 模式) ======
echo "==> Starting RawWise (profile=$PROFILE, Embedded)..."
"$RAW_LINK" \
  $JVM_BASE \
  -Xmx$EMBEDDED_HEAP -XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY \
  -Drawwise.warehouse.path="$WAREHOUSE_PATH" \
  -Drawwise.storage.lmdb.initial-map-size-mb="$STORAGE_LMDB_INIT_MB" \
  -Drawwise.storage.lmdb.max-map-size-mb="$STORAGE_LMDB_MAX_MB" \
  -Drawwise.metastore.type="${RAWWISE_METASTORE_TYPE:-rocksdb}" \
  -Drawwise.metastore.base-path="$METASTORE_PATH" \
  -Drawwise.metastore.grpc-port="$METASTORE_PORT" \
  -Drawwise.auth.enabled=false \
  -Drawwise.auth.explicitly-unsafe-disable=true \
  -Drawwise.auth.token-dir="$TOKEN_DIR" \
  -Drawwise.axiom.orchestrator.enabled=false \
  -Drawwise.axiom.scheduler.enabled=false \
  -Drawwise.axiom.scheduler.warmup-on-start=false \
  -Drawwise.axiom.scheduler.consolidate.enabled=false \
  -Drawwise.context.enabled=true \
  -Drawwise.frontend.ir.enabled=true \
  -Drawwise.frontend.grpc.tls.enabled=false \
  -Drawwise.grpc.tls.explicitly-unsafe-disable=true \
  -Drawwise.backend.tls.enabled=false \
  -Drawwise.updateLog.base-path="$DATA_DIR/update_logs" \
  -Drawwise.embedding.provider.type="$RAWWISE_EMBEDDING_TYPE" \
  -Drawwise.embedding.provider.endpoint="$RAWWISE_EMBEDDING_ENDPOINT" \
  -Drawwise.embedding.provider.model="$RAWWISE_EMBEDDING_MODEL" \
  -Drawwise.embedding.provider.dimension="$RAWWISE_EMBEDDING_DIMENSION" \
  -Drawwise.embedding.provider.api-key="$RAWWISE_EMBEDDING_API_KEY" \
  -Drawwise.embedding.provider.timeout-ms="$RAWWISE_EMBEDDING_TIMEOUT_MS" \
  -Drawwise.embedding.provider.max-retries="$RAWWISE_EMBEDDING_MAX_RETRIES" \
  -Drawwise.embedding.provider.batch-size="$RAWWISE_EMBEDDING_BATCH_SIZE" \
  -Drawwise.embedding.provider.format="$RAWWISE_EMBEDDING_FORMAT" \
  -Drawwise.summarization.provider.type="$RAWWISE_SUMMARIZATION_TYPE" \
  -Drawwise.summarization.provider.endpoint="$RAWWISE_SUMMARIZATION_ENDPOINT" \
  -Drawwise.summarization.provider.model="$RAWWISE_SUMMARIZATION_MODEL" \
  -Drawwise.summarization.provider.api-key="$RAWWISE_SUMMARIZATION_API_KEY" \
  -Drawwise.axiom.multiquery.enabled="${RAWWISE_MULTIQUERY_ENABLED:-true}" \
  -Drawwise.axiom.multiquery.expand.n="${RAWWISE_MULTIQUERY_EXPAND_N:-5}" \
  -Drawwise.axiom.derivation.async.concurrency="$RAWWISE_AXIOM_DERIVATION_ASYNC_CONCURRENCY" \
  -Drawwise.axiom.rrf.k="${RAWWISE_RRF_K:-30}" \
  -Drawwise.axiom.search.topk-multiplier="${RAWWISE_SEARCH_TOPK_MULTIPLIER:-3}" \
  -Drawwise.axiom.graph.rescue.enabled="${RAWWISE_GRAPH_RESCUE_ENABLED:-false}" \
  -Drawwise.axiom.search.path="${RAWWISE_SEARCH_PATH:-hybrid}" \
  -Drawwise.graph.shard-coordinator.enabled="${RAWWISE_SHARD_COORDINATOR_ENABLED:-true}" \
  -Drawwise.axiom.graph.boost="${RAWWISE_GRAPH_BOOST:-0.1}" \
  -Drawwise.axiom.l0summarization.enabled="${RAWWISE_L0_SUMMARIZATION_ENABLED:-false}" \
  -Drawwise.axiom.l0summarization.endpoint="${RAWWISE_L0_SUMMARIZATION_ENDPOINT:-}" \
  -Drawwise.axiom.l0summarization.api-key="${RAWWISE_L0_SUMMARIZATION_API_KEY:-}" \
  -Drawwise.axiom.l0summarization.model="${RAWWISE_L0_SUMMARIZATION_MODEL:-gpt-4o-mini}" \
  -Drawwise.axiom.fact.extraction.enabled="${RAWWISE_FACT_EXTRACTION_ENABLED:-false}" \
  -Drawwise.axiom.fact.max-per-record="${RAWWISE_FACT_MAX_PER_RECORD:-5}" \
  -Drawwise.axiom.fact.extraction.timeout-ms="${RAWWISE_FACT_EXTRACTION_TIMEOUT_MS:-30000}" \
  -Drawwise.axiom.fact.min-len="${RAWWISE_FACT_MIN_LEN:-8}" \
  -Drawwise.axiom.fact.min-source-len="${RAWWISE_FACT_MIN_SOURCE_LEN:-0}" \
  -Drawwise.axiom.fact.dedup-mode="${RAWWISE_FACT_DEDUP_MODE:-D}" \
  -Drawwise.axiom.fact.concurrency="${RAWWISE_FACT_CONCURRENCY:-4}" \
  -Drawwise.entity.extraction.llm.api-key="${RAWWISE_ENTITY_LLM_API_KEY:-}" \
  -Drawwise.entity.extraction.llm.model="${RAWWISE_ENTITY_LLM_MODEL:-default}" \
  -Drawwise.entity.extraction.enabled="${RAWWISE_ENTITY_EXTRACTION_ENABLED:-false}" \
  -Drawwise.entity.extraction.llm.provider="${RAWWISE_ENTITY_LLM_PROVIDER:-stub://}" \
  -Drawwise.axiom.queryreformulation.enabled="${RAWWISE_QUERY_REFORMULATION_ENABLED:-false}" \
  -Drawwise.axiom.queryreformulation.endpoint="${RAWWISE_QUERY_REFORMULATION_ENDPOINT:-}" \
  -Drawwise.axiom.queryreformulation.api-key="${RAWWISE_QUERY_REFORMULATION_API_KEY:-}" \
  -Drawwise.axiom.queryreformulation.model="${RAWWISE_QUERY_REFORMULATION_MODEL:-gpt-4o-mini}" \
  -Drawwise.axiom.query-rewrite.enabled="${RAWWISE_QUERY_REWRITE_ENABLED:-false}" \
  -Drawwise.axiom.query-rewrite.endpoint="${RAWWISE_QUERY_REWRITE_ENDPOINT:-}" \
  -Drawwise.axiom.query-rewrite.api-key="${RAWWISE_QUERY_REWRITE_API_KEY:-}" \
  -Drawwise.axiom.query-rewrite.model="${RAWWISE_QUERY_REWRITE_MODEL:-deepseek-chat}" \
  -Drawwise.axiom.query-rewrite.timeout-ms="${RAWWISE_QUERY_REWRITE_TIMEOUT_MS:-5000}" \
  -Drawwise.axiom.reranker.enabled="${RAWWISE_RERANKER_ENABLED:-false}" \
  -Drawwise.axiom.reranker.endpoint="${RAWWISE_RERANKER_ENDPOINT:-}" \
  -Drawwise.axiom.reranker.api-key="${RAWWISE_RERANKER_API_KEY:-}" \
  -Drawwise.axiom.reranker.model="${RAWWISE_RERANKER_MODEL:-gpt-4o-mini}" \
  -Drawwise.axiom.reranker.timeout-ms="${RAWWISE_RERANKER_TIMEOUT_MS:-5000}" \
  -Drawwise.axiom.crossencoder.alpha="${RAWWISE_CROSSENCODER_ALPHA:-0.6}" \
  -Drawwise.axiom.feature-rerank.enabled="${RAWWISE_FEATURE_RERANK_ENABLED:-true}" \
  -Drawwise.axiom.feature-rerank.vec-weight="${RAWWISE_FEATURE_RERANK_VEC_WEIGHT:-0.30}" \
  -Drawwise.axiom.feature-rerank.kw-weight="${RAWWISE_FEATURE_RERANK_KW_WEIGHT:-0.30}" \
  -Drawwise.axiom.feature-rerank.graph-weight="${RAWWISE_FEATURE_RERANK_GRAPH_WEIGHT:-0.15}" \
  -Drawwise.axiom.feature-rerank.multiquery-weight="${RAWWISE_FEATURE_RERANK_MULTIQUERY_WEIGHT:-0.10}" \
  -Drawwise.axiom.feature-rerank.subject-boost="${RAWWISE_FEATURE_RERANK_SUBJECT_BOOST:-0.0}" \
  -Drawwise.axiom.entity-aware.enabled="${RAWWISE_ENTITY_AWARE_ENABLED:-false}" \
  -Drawwise.axiom.feedback.llm.enabled="${RAWWISE_FEEDBACK_LLM_ENABLED:-false}" \
  -Drawwise.axiom.feedback.llm.endpoint="${RAWWISE_FEEDBACK_LLM_ENDPOINT:-}" \
  -Drawwise.axiom.feedback.llm.api-key="${RAWWISE_FEEDBACK_LLM_API_KEY:-}" \
  -Drawwise.axiom.feedback.llm.model="${RAWWISE_FEEDBACK_LLM_MODEL:-gpt-4o-mini}" \
  -Drawwise.axiom.cascade.llm.enabled="${RAWWISE_CASCADE_LLM_ENABLED:-false}" \
  -Drawwise.axiom.cascade.llm.endpoint="${RAWWISE_CASCADE_LLM_ENDPOINT:-}" \
  -Drawwise.axiom.cascade.llm.api-key="${RAWWISE_CASCADE_LLM_API_KEY:-}" \
  -Drawwise.axiom.cascade.llm.model="${RAWWISE_CASCADE_LLM_MODEL:-gpt-4o-mini}" \
  -Drawwise.axiom.semantic.session.enabled="${RAWWISE_SEMANTIC_SESSION_ENABLED:-false}" \
  -Drawwise.axiom.semantic.session.delay-ms="${RAWWISE_SEMANTIC_SESSION_DELAY_MS:-15000}" \
  -Drawwise.axiom.semantic.session.timeout-ms="${RAWWISE_SEMANTIC_SESSION_TIMEOUT_MS:-60000}" \
  -Drawwise.axiom.semantic.session.min-records="${RAWWISE_SEMANTIC_SESSION_MIN_RECORDS:-2}" \
  -Drawwise.axiom.semantic.session.force-regen="${RAWWISE_SEMANTIC_SESSION_FORCE_REGEN:-false}" \
  -Drawwise.axiom.semantic.boost-value="${RAWWISE_SEMANTIC_BOOST_VALUE:-0.01}" \
  -Drawwise.frontend.rate-limit.per-ip="$RAWWISE_RATE_LIMIT_PER_IP" \
  -Drawwise.frontend.rate-limit.global="$RAWWISE_RATE_LIMIT_GLOBAL" \
  -cp "$CP" \
  io.github.rawwise.frontend.server.Main \
    --embedded --port "$FRONTEND_PORT" \
  > "$RAW_LOG" 2>&1 &
RAW_PID=$!
echo "$RAW_PID" > "$PID_FILE"
echo "   PID=$RAW_PID (written to $PID_FILE)"

sleep 3

# ====== 健康检查 ======
echo "==> Checking health (port $FRONTEND_PORT)..."
HEALTH_OK=0
for i in $(seq 1 30); do
    HTTP_OK=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$FRONTEND_PORT/" 2>/dev/null || echo "000")
    if [ "$HTTP_OK" != "000" ]; then
        HEALTH_OK=1
        break
    fi
    sleep 1
done

if [ "$HEALTH_OK" = "1" ]; then
    echo "==> RawWise (profile=$PROFILE) is running! (Embedded mode — InProcess gRPC, single JVM)"
    echo "    HTTP API:  http://localhost:$FRONTEND_PORT/api/query"
    echo "    Axiom:     http://localhost:$FRONTEND_PORT/api/axiom/ingest"
    echo "    Metastore: localhost:$METASTORE_PORT (RocksDB)"
    echo "    Backend:   InProcess (no TCP port)"
    echo "    Executor:  InProcess (no TCP port)"
    echo "    Data:      $DATA_DIR  (persistent)"
    echo "    Log:       $RAW_LOG"
    echo "    PID:       $RAW_PID  ($PID_FILE)"
    echo "    Stop:      bash bin/rawwise-stop.sh --profile=$PROFILE"
    echo "    Clean:     bash bin/rawwise-clean.sh --profile=$PROFILE"
else
    echo "WARN: RawWise (profile=$PROFILE) not responding after 30s, check $RAW_LOG"
    echo "      Last 20 lines:"
    tail -20 "$RAW_LOG" 2>/dev/null || true
fi

if [ "${1:-}" = "--foreground" ]; then
    echo "==> Running in foreground (Ctrl+C to stop)..."
    wait
fi

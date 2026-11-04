#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)/bin"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONF_FILE="$SCRIPT_DIR/standalone-cluster.conf"
CONF_EXAMPLE="$SCRIPT_DIR/standalone-cluster.conf.example"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $*"; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [worker_count]

Start a RawWise Standalone local cluster with Spark Standalone cluster manager.

Architecture:
  Spark Master (1 process, :7077 + WebUI :8088)
    ├── Spark Worker 1 → Backend Node 1 (JVM)
    ├── Spark Worker 2 → Backend Node 2 (JVM)
    ├── ...
    └── Spark Worker N → Backend Node N (JVM)
  
  Backend Server (Spark Driver, :9090 gRPC)
    └── Connects to spark://localhost:7077
  
  Frontend (HTTP :8080, embeds Metastore :9091 in dev mode)

Each Worker is a separate JVM process. Workers = Backend Nodes = $N processes.

Examples:
  $(basename "$0")        # Use worker_count from config (default: 4)
  $(basename "$0") 2      # Override: 2 Workers (2 Backend Nodes)
  $(basename "$0") 8      # Override: 8 Workers (8 Backend Nodes)

Configuration:
  Copy standalone-cluster.conf.example to standalone-cluster.conf and customize.
  Environment variables override config values.
EOF
}

# ---- Load Configuration ----

load_conf() {
    if [ -f "$CONF_FILE" ]; then
        source "$CONF_FILE"
        log_info "Loaded: $CONF_FILE"
    elif [ -f "$CONF_EXAMPLE" ]; then
        log_warn "standalone-cluster.conf not found, using .example"
        source "$CONF_EXAMPLE"
    else
        log_error "No config found at $SCRIPT_DIR"
        exit 1
    fi
}

load_conf

# CLI override for worker count
if [ $# -ge 1 ] && [[ "$1" =~ ^[0-9]+$ ]]; then
    WORKER_COUNT="$1"
fi

# Defaults
WORKER_COUNT="${WORKER_COUNT:-4}"
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEMORY="${WORKER_MEMORY:-2g}"
SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8088}"
BACKEND_PORT="${BACKEND_PORT:-9090}"
BACKEND_SPARK_MASTER="${BACKEND_SPARK_MASTER:-spark://localhost:7077}"
BACKEND_HEAP="${BACKEND_HEAP:-4g}"
BACKEND_DIRECT_MEMORY="${BACKEND_DIRECT_MEMORY:-2g}"
FRONTEND_PORT="${FRONTEND_PORT:-8080}"
FRONTEND_HEAP="${FRONTEND_HEAP:-2g}"
METASTORE_MODE="${METASTORE_MODE:-embedded}"
METASTORE_PORT="${METASTORE_PORT:-9091}"
METASTORE_TYPE="${METASTORE_TYPE:-rocksdb}"
METASTORE_BASE_PATH="${METASTORE_BASE_PATH:-/tmp/rawwise/metastore}"
WAREHOUSE_PATH="${WAREHOUSE_PATH:-/tmp/rawwise/warehouse}"
IR_ENABLED="${IR_ENABLED:-true}"
AUTH_ENABLED="${AUTH_ENABLED:-false}"

# ---- SPARK_HOME Detection ----

detect_spark_home() {
    if [ -n "${SPARK_HOME:-}" ] && [ -f "$SPARK_HOME/sbin/start-master.sh" ]; then
        return 0
    fi

    # Homebrew on macOS
    if command -v brew &>/dev/null; then
        local brew_spark
        brew_spark=$(brew --prefix apache-spark 2>/dev/null || true)
        if [ -n "$brew_spark" ] && [ -f "$brew_spark/libexec/sbin/start-master.sh" ]; then
            SPARK_HOME="$brew_spark/libexec"
            log_info "Detected Spark (Homebrew): $SPARK_HOME"
            return 0
        fi
    fi

    # Common install paths
    for candidate in /opt/spark /usr/local/spark /usr/local/opt/apache-spark/libexec "$HOME/spark"; do
        if [ -f "$candidate/sbin/start-master.sh" ]; then
            SPARK_HOME="$candidate"
            log_info "Detected Spark: $SPARK_HOME"
            return 0
        fi
    done

    log_error "SPARK_HOME not set and Spark not found in common paths."
    log_error "Set SPARK_HOME environment variable or install Spark:"
    log_error "  brew install apache-spark   # macOS"
    log_error "  export SPARK_HOME=/path/to/spark"
    exit 1
}

detect_spark_home

SPARK_SBIN="$SPARK_HOME/sbin"
SPARK_MASTER_URL="spark://localhost:$SPARK_MASTER_PORT"

# ---- PIDs and Logs ----

LOG_DIR="$PROJECT_DIR/${LOG_DIR:-logs}"
PID_DIR="$PROJECT_DIR/${PID_DIR:-run}"
mkdir -p "$LOG_DIR" "$PID_DIR"

MASTER_PID_FILE="$PID_DIR/spark-master.pid"
WORKER_PID_FILES=()
BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
METASTORE_PID_FILE="$PID_DIR/metastore.pid"

# ---- Java Setup ----

JVM_BASE="--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.invoke=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
-XX:+UseZGC -XX:+ZGenerational"

compute_classpath() {
    log_info "Computing classpath..."
    mvn dependency:build-classpath -pl rawwise-core \
        -Dmdep.outputFile=/tmp/rawwise/cp-full.txt -q 2>/dev/null
    CP="$(cat /tmp/rawwise/cp-full.txt):$PROJECT_DIR/rawwise-core/target/classes"
    log_info "Classpath ready ($(echo "$CP" | tr ':' '\n' | wc -l | tr -d ' ') entries)"
}

# ---- Spark Master Lifecycle ----

start_spark_master() {
    log_step "Starting Spark Master..."
    if [ -f "$MASTER_PID_FILE" ] && kill -0 "$(cat "$MASTER_PID_FILE")" 2>/dev/null; then
        log_warn "Spark Master already running (PID $(cat "$MASTER_PID_FILE"))"
        return 0
    fi

    "$SPARK_SBIN/start-master.sh" \
        --port "$SPARK_MASTER_PORT" \
        --webui-port "$SPARK_MASTER_WEBUI_PORT" \
        > "$LOG_DIR/spark-master.out" 2>&1

    # Detect PID from Spark's pid file
    local pid
    pid=$(cat "$SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-*.pid" 2>/dev/null | head -1 || true)
    if [ -z "$pid" ]; then
        pid=$(pgrep -f "org.apache.spark.deploy.master.Master" | head -1 || true)
    fi
    if [ -z "$pid" ]; then
        log_error "Failed to detect Spark Master PID"
        return 1
    fi
    echo "$pid" > "$MASTER_PID_FILE"

    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        log_info "Spark Master started (PID $pid, port $SPARK_MASTER_PORT, WebUI :$SPARK_MASTER_WEBUI_PORT)"
    else
        log_error "Spark Master failed to start"
        return 1
    fi
}

stop_spark_master() {
    if [ -f "$MASTER_PID_FILE" ]; then
        local pid
        pid=$(cat "$MASTER_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping Spark Master (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$MASTER_PID_FILE"
    fi
}

# ---- Spark Worker Lifecycle ----

start_worker() {
    local idx="$1"
    local worker_log="$LOG_DIR/spark-worker-${idx}.out"
    local worker_dir="$PROJECT_DIR/work/worker-${idx}"

    mkdir -p "$worker_dir"

    log_step "Starting Spark Worker $idx (cores=$WORKER_CORES, memory=$WORKER_MEMORY)..."
    "$SPARK_SBIN/start-worker.sh" \
        "$SPARK_MASTER_URL" \
        --cores "$WORKER_CORES" \
        --memory "$WORKER_MEMORY" \
        --work-dir "$worker_dir" \
        > "$worker_log" 2>&1

    sleep 1
    local pid
    pid=$(pgrep -f "org.apache.spark.deploy.worker.Worker.*$SPARK_MASTER_URL" | tail -1 || true)
    if [ -n "$pid" ]; then
        WORKER_PID_FILES+=("$pid")
        log_info "  Worker $idx started (PID $pid)"
    else
        log_warn "  Worker $idx may not have started (check $worker_log)"
    fi
}

stop_all_workers() {
    log_info "Stopping all Spark Workers..."
    for pid in "${WORKER_PID_FILES[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    sleep 1
    # Force kill any remaining workers
    pkill -f "org.apache.spark.deploy.worker.Worker" 2>/dev/null || true
    log_info "All Workers stopped"
}

# ---- Metastore Lifecycle ----

start_metastore() {
    if [ "$METASTORE_MODE" != "standalone" ]; then
        log_info "Metastore mode: embedded (in Frontend)"
        return 0
    fi

    log_step "Starting Standalone Metastore (:${METASTORE_PORT}, type=$METASTORE_TYPE)..."
    "$BIN_DIR/metastore" start \
        --port "$METASTORE_PORT" \
        --metastore "$METASTORE_TYPE"
}

stop_metastore() {
    if [ "$METASTORE_MODE" = "standalone" ]; then
        "$BIN_DIR/metastore" stop 2>/dev/null || true
    fi
}

# ---- Backend Server Lifecycle ----

start_backend() {
    log_step "Starting Backend Server (Driver)..."

    if [ -f "$BACKEND_PID_FILE" ] && kill -0 "$(cat "$BACKEND_PID_FILE")" 2>/dev/null; then
        log_warn "Backend Server already running (PID $(cat "$BACKEND_PID_FILE"))"
        return 0
    fi

    local metastore_args
    if [ "$METASTORE_MODE" = "standalone" ]; then
        metastore_args="-Drawwise.metastore.type=grpc -Drawwise.metastore.grpc-host=localhost -Drawwise.metastore.grpc-port=$METASTORE_PORT"
    else
        metastore_args="-Drawwise.metastore.type=grpc -Drawwise.metastore.grpc-host=localhost -Drawwise.metastore.grpc-port=$METASTORE_PORT"
    fi

    nohup java \
        $JVM_BASE \
        -Xmx"$BACKEND_HEAP" \
        -XX:MaxDirectMemorySize="$BACKEND_DIRECT_MEMORY" \
        -Drawwise.backend.spark.master="$BACKEND_SPARK_MASTER" \
        -Drawwise.warehouse.path="$WAREHOUSE_PATH" \
        $metastore_args \
        -Drawwise.auth.explicitly-unsafe-disable=true \
        -Drawwise.backend.auth.enabled=false \
        -Drawwise.backend.tls.enabled=false \
        -cp "$CP" \
        io.github.rawwise.backend.server.Main \
        --port "$BACKEND_PORT" \
        > "$LOG_DIR/backend.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$BACKEND_PID_FILE"

    log_info "  Backend Server starting (PID $pid, gRPC :$BACKEND_PORT)"
    log_info "  Spark Driver connects to $BACKEND_SPARK_MASTER"

    # Wait for gRPC port
    local waited=0
    while ! lsof -i ":$BACKEND_PORT" >/dev/null 2>&1 && [ $waited -lt 60 ]; do
        sleep 2
        waited=$((waited + 2))
    done
    if [ $waited -ge 60 ]; then
        log_warn "  Backend gRPC port not ready after 60s (check $LOG_DIR/backend.log)"
    else
        log_info "  Backend Server ready (gRPC :$BACKEND_PORT)"
    fi
}

stop_backend() {
    if [ -f "$BACKEND_PID_FILE" ]; then
        local pid
        pid=$(cat "$BACKEND_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping Backend Server (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 3
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$BACKEND_PID_FILE"
    fi
}

# ---- Frontend Lifecycle ----

start_frontend() {
    log_step "Starting Frontend Server (HTTP :$FRONTEND_PORT -> Backend :$BACKEND_PORT)..."

    if [ -f "$FRONTEND_PID_FILE" ] && kill -0 "$(cat "$FRONTEND_PID_FILE")" 2>/dev/null; then
        log_warn "Frontend already running (PID $(cat "$FRONTEND_PID_FILE"))"
        return 0
    fi

    local metastore_config
    if [ "$METASTORE_MODE" = "standalone" ]; then
        # Frontend connects to standalone metastore via gRPC
        metastore_config="-Drawwise.metastore.type=grpc -Drawwise.metastore.grpc-host=localhost -Drawwise.metastore.grpc-port=$METASTORE_PORT"
    else
        # Frontend embeds RocksDB metastore, exposes gRPC on METASTORE_PORT
        metastore_config="-Drawwise.metastore.type=$METASTORE_TYPE -Drawwise.metastore.base-path=$METASTORE_BASE_PATH"
    fi

    nohup java \
        $JVM_BASE \
        -Xmx"$FRONTEND_HEAP" \
        -Drawwise.warehouse.path="$WAREHOUSE_PATH" \
        $metastore_config \
        -Drawwise.auth.enabled="$AUTH_ENABLED" \
        -Drawwise.auth.explicitly-unsafe-disable=true \
        -Drawwise.auth.token-dir=/tmp/rawwise \
        -Drawwise.axiom.orchestrator.enabled=false \
        -Drawwise.frontend.ir.enabled="$IR_ENABLED" \
        -Drawwise.frontend.grpc.tls.enabled=false \
        -cp "$CP" \
        io.github.rawwise.frontend.server.Main \
        --port "$FRONTEND_PORT" --backend-host localhost --backend-port "$BACKEND_PORT" \
        > "$LOG_DIR/frontend.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$FRONTEND_PID_FILE"

    # Wait for HTTP port
    local waited=0
    while ! lsof -i ":$FRONTEND_PORT" >/dev/null 2>&1 && [ $waited -lt 30 ]; do
        sleep 1
        waited=$((waited + 1))
    done
    if [ $waited -ge 30 ]; then
        log_warn "  Frontend not ready after 30s (check $LOG_DIR/frontend.log)"
    else
        log_info "  Frontend ready (HTTP :$FRONTEND_PORT)"
    fi
}

stop_frontend() {
    if [ -f "$FRONTEND_PID_FILE" ]; then
        local pid
        pid=$(cat "$FRONTEND_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping Frontend (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$FRONTEND_PID_FILE"
    fi
}

# ---- Cleanup ----

cleanup() {
    echo ""
    log_warn "Shutting down RawWise Standalone Cluster..."
    stop_frontend
    stop_backend
    stop_metastore
    stop_all_workers
    stop_spark_master
    log_info "All processes stopped"
}
trap cleanup EXIT INT TERM

# ---- Main ----

echo ""
log_step "================================================"
log_step "  RawWise Standalone Local Cluster"
log_step "================================================"
log_info "Spark Home:     $SPARK_HOME"
log_info "Spark Master:   spark://localhost:$SPARK_MASTER_PORT (WebUI :$SPARK_MASTER_WEBUI_PORT)"
log_info "Workers:        $WORKER_COUNT (cores=$WORKER_CORES, memory=$WORKER_MEMORY each)"
log_info "Backend Server: gRPC :$BACKEND_PORT (heap=$BACKEND_HEAP, direct=$BACKEND_DIRECT_MEMORY)"
log_info "Frontend:       HTTP :$FRONTEND_PORT (heap=$FRONTEND_HEAP)"
log_info "Metastore:      $METASTORE_MODE (type=$METASTORE_TYPE, port=$METASTORE_PORT)"
log_info "Warehouse:      $WAREHOUSE_PATH"
echo ""

compute_classpath

# Start in dependency order
start_spark_master

for i in $(seq 1 "$WORKER_COUNT"); do
    start_worker "$i"
done

sleep 2

# Backend Server = Spark Driver, connects to spark://localhost:7077
# Metastore: Backend connects via gRPC to Frontend's embedded metastore (:9091)
start_backend

sleep 2
start_frontend

echo ""
log_step "================================================"
log_step "  Cluster Ready"
log_step "================================================"
log_info "Frontend:     http://localhost:$FRONTEND_PORT"
log_info "Backend:      grpc://localhost:$BACKEND_PORT"
log_info "Spark Master: http://localhost:$SPARK_MASTER_WEBUI_PORT"
log_info "Metastore:    grpc://localhost:$METASTORE_PORT ($METASTORE_MODE)"
log_info ""
log_info "Backend Nodes ($WORKER_COUNT Spark Workers):"
for i in $(seq 1 "$WORKER_COUNT"); do
    log_info "  Worker $i: $PROJECT_DIR/work/worker-$i"
done
log_info ""
log_info "Logs: $LOG_DIR"
log_info "PIDs:  $PID_DIR"
log_info ""
log_info "Press Ctrl+C to stop all services"

# Keep running until Ctrl+C
wait

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/target"
LOG_DIR="$PROJECT_DIR/logs"
CONF_DIR="${RAWWISE_CONF_DIR:-$PROJECT_DIR/conf}"
CP_CACHE_FILE="$BUILD_DIR/.classpath-cache"

FRONTEND_PORT=8080
RAW_PID_FILE="$LOG_DIR/rawwise.pid"

CONFIG_FILE=""
SPARK_MASTER=""
METASTORE_TYPE=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $*"; }

build() {
    log_step "Building project..."
    cd "$PROJECT_DIR"
    mvn compile -DskipTests -q
    if [ $? -ne 0 ]; then
        log_error "Build failed! Check errors above."
        exit 1
    fi
    rm -f "$CP_CACHE_FILE"
    log_info "Build successful"
}

get_classpath() {
    if [ -f "$CP_CACHE_FILE" ]; then
        cat "$CP_CACHE_FILE"
        return
    fi

    log_info "Resolving classpath (caching for reuse)..."
    cd "$PROJECT_DIR"
    mvn dependency:build-classpath -Dmdep.outputFile="$CP_CACHE_FILE.tmp" -pl rawwise-core -q 2>/dev/null
    echo "$PROJECT_DIR/rawwise-core/target/classes:$PROJECT_DIR/rawwise-benchmark/target/classes:$(cat "$CP_CACHE_FILE.tmp")" > "$CP_CACHE_FILE"
    rm -f "$CP_CACHE_FILE.tmp"
    cat "$CP_CACHE_FILE"
}

wait_for_port() {
    local port=$1
    local name=$2
    local max_wait=${3:-30}
    local waited=0
    while ! lsof -i :"$port" >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge $max_wait ]; then
            log_error "$name did not start within ${max_wait}s"
            log_error "Check logs: $LOG_DIR/"
            return 1
        fi
    done
}

wait_for_health() {
    local url=$1
    local name=$2
    local max_wait=${3:-30}
    local waited=0
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge $max_wait ]; then
            log_warn "$name health check not passing after ${max_wait}s"
            return 1
        fi
    done
    log_info "$name is healthy"
}

start_rawwise() {
    if [ -f "$RAW_PID_FILE" ] && kill -0 "$(cat "$RAW_PID_FILE")" 2>/dev/null; then
        log_warn "RawWise already running (PID $(cat "$RAW_PID_FILE"))"
        return 0
    fi

    log_step "Starting RawWise (Embedded mode)..."
    mkdir -p "$LOG_DIR"

    local cp
    cp=$(get_classpath)

    local java_opts=""
    java_opts="$java_opts --add-opens=java.base/java.nio=ALL-UNNAMED"
    java_opts="$java_opts --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    java_opts="$java_opts --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    java_opts="$java_opts --add-opens=java.base/java.util=ALL-UNNAMED"

    if [ -n "$CONFIG_FILE" ]; then
        java_opts="$java_opts -Drawwise.config.file=$CONFIG_FILE"
    fi

    local cli_args="--embedded --port $FRONTEND_PORT"
    if [ -n "$METASTORE_TYPE" ]; then
        cli_args="$cli_args --metastore $METASTORE_TYPE"
    fi

    nohup java $java_opts \
        -Xmx6g -XX:MaxDirectMemorySize=2g \
        -Drawwise.metastore.type=rocksdb \
        -Drawwise.metastore.base-path="$HOME/.rawwise/data/metastore" \
        -Drawwise.warehouse.path="$HOME/.rawwise/data/warehouse" \
        -Drawwise.auth.enabled=false \
        -Drawwise.auth.explicitly-unsafe-disable=true \
        -cp "$cp" \
        io.github.rawwise.frontend.server.Main \
        $cli_args \
        > "$LOG_DIR/rawwise.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$RAW_PID_FILE"

    wait_for_port "$FRONTEND_PORT" "RawWise" 60
    wait_for_health "http://localhost:$FRONTEND_PORT/health" "RawWise" 10
    log_info "RawWise started (PID $pid, HTTP port $FRONTEND_PORT, Backend gRPC $BACKEND_PORT)"
}

start_repl() {
    local host=${1:-localhost}
    local port=${2:-$FRONTEND_PORT}
    local database=${3:-default}

    log_step "Starting RawWise REPL (connecting to $host:$port)..."
    local cp
    cp=$(get_classpath)

    java \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        -cp "$cp" \
        io.github.rawwise.client.repl.RawWiseRepl \
        --host "$host" --port "$port" --database "$database"
}

stop_rawwise() {
    if [ ! -f "$RAW_PID_FILE" ]; then
        log_warn "RawWise is not running (no PID file)"
        return 0
    fi

    local pid
    pid=$(cat "$RAW_PID_FILE")

    if ! kill -0 "$pid" 2>/dev/null; then
        log_warn "RawWise is not running (PID $pid stale)"
        rm -f "$RAW_PID_FILE"
        return 0
    fi

    log_info "Stopping RawWise (PID $pid)..."
    kill "$pid" 2>/dev/null || true

    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ $waited -lt 15 ]; do
        sleep 1
        waited=$((waited + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        log_warn "RawWise did not stop gracefully, force killing..."
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$RAW_PID_FILE"
    log_info "RawWise stopped"
}

status() {
    echo ""
    echo "RawWise Server Status:"
    echo "======================="

    if [ -f "$RAW_PID_FILE" ] && kill -0 "$(cat "$RAW_PID_FILE")" 2>/dev/null; then
        echo -e "  RawWise: ${GREEN}RUNNING${NC} (PID $(cat "$RAW_PID_FILE"), Embedded InProcess)"
        echo -e "    HTTP:     http://localhost:$FRONTEND_PORT"
        echo -e "    Backend:  InProcess gRPC (no TCP port)"
        echo -e "    Executor: InProcess gRPC (no TCP port)"
    else
        echo -e "  RawWise: ${RED}STOPPED${NC}"
    fi

    echo ""
}

logs() {
    local log_file="$LOG_DIR/rawwise.log"
    if [ -f "$log_file" ]; then
        tail -f "$log_file"
    else
        log_error "No log found at $log_file"
    fi
}

usage() {
    cat <<EOF
RawWise Local Development Script (Embedded mode)

Usage: $(basename "$0") <command> [options]

Commands:
  build       Build the project
  start       Build + Start RawWise (single JVM embedded)
  stop        Stop RawWise
  restart     Stop and restart RawWise
  repl        Start the REPL client (assumes server is running)
  up          Build + Start + REPL (full stack)
  quick       Start + REPL (skip build, use cached classpath)
  status      Show server status
  logs        Tail server log

Options for 'start', 'restart', 'up', 'quick':
  --config <file>      Path to rawwise.conf
  --master <url>       Spark master URL (e.g. local[4])
  --metastore <kind>   Metastore type (inmemory|rocksdb)

Options for 'repl', 'up', 'quick':
  --host <host>       Frontend host (default: localhost)
  --port <port>       Frontend port (default: $FRONTEND_PORT)
  --database <db>     Default database (default: default)

Examples:
  $(basename "$0") up                    # Full stack: build, start, launch REPL
  $(basename "$0") quick                 # Skip build, start + REPL
  $(basename "$0") start                 # Build + Start
  $(basename "$0") start --master local[8]  # Custom Spark master
  $(basename "$0") repl                  # Connect REPL to running server
  $(basename "$0") logs                  # Tail server log
  $(basename "$0") stop                  # Stop server
EOF
}

parse_global_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)    CONFIG_FILE="$2"; shift 2 ;;
            --master)    SPARK_MASTER="$2"; shift 2 ;;
            --metastore) METASTORE_TYPE="$2"; shift 2 ;;
            *)           break ;;
        esac
    done
}

parse_repl_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --host)     REPL_HOST="$2"; shift 2 ;;
            --port)     REPL_PORT="$2"; shift 2 ;;
            --database) REPL_DB="$2"; shift 2 ;;
            *)          shift ;;
        esac
    done
}

REPL_HOST="localhost"
REPL_PORT="$FRONTEND_PORT"
REPL_DB="default"

case "${1:-}" in
    build)
        build
        ;;
    start)
        parse_global_args "${@:2}"
        build
        start_rawwise
        status
        ;;
    stop)
        stop_rawwise
        ;;
    restart)
        parse_global_args "${@:2}"
        stop_rawwise
        build
        start_rawwise
        status
        ;;
    repl)
        parse_repl_args "${@:2}"
        start_repl "$REPL_HOST" "$REPL_PORT" "$REPL_DB"
        ;;
    up)
        parse_global_args "${@:2}"
        build
        start_rawwise
        start_repl "$REPL_HOST" "$REPL_PORT" "$REPL_DB"
        ;;
    quick)
        parse_global_args "${@:2}"
        start_rawwise
        start_repl "$REPL_HOST" "$REPL_PORT" "$REPL_DB"
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    *)
        usage
        exit 1
        ;;
esac

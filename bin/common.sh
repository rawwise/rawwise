#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/target"
LOG_DIR="$PROJECT_DIR/logs"
CONF_DIR="${RAWWISE_CONF_DIR:-$PROJECT_DIR/conf}"
PID_DIR="${RAWWISE_PID_DIR:-$PROJECT_DIR/run}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $*"; }

resolve_jar() {
    local jar_pattern="${1:-rawwise-*.jar}"
    local jar_file
    jar_file=$(find "$BUILD_DIR" -maxdepth 1 -name "$jar_pattern" -print -quit 2>/dev/null || true)
    if [ -z "$jar_file" ]; then
        log_error "No JAR found matching pattern: $jar_pattern in $BUILD_DIR"
        log_error "Run 'mvn package -DskipTests' first"
        exit 1
    fi
    echo "$jar_file"
}

resolve_config() {
    local config_file="${1:-}"
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        echo "$config_file"
        return
    fi
    if [ -f "$CONF_DIR/rawwise.conf" ]; then
        echo "$CONF_DIR/rawwise.conf"
        return
    fi
    echo ""
}

build_java_opts() {
    local config_file="${1:-}"
    local opts=""
    opts="$opts --add-opens=java.base/java.nio=ALL-UNNAMED"
    opts="$opts --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    opts="$opts --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    opts="$opts --add-opens=java.base/java.util=ALL-UNNAMED"
    if [ -n "$config_file" ]; then
        opts="$opts -Drawwise.config.file=$config_file"
    fi
    echo "$opts"
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

is_running() {
    local pid_file=$1
    if [ ! -f "$pid_file" ]; then
        return 1
    fi
    local pid
    pid=$(cat "$pid_file")
    kill -0 "$pid" 2>/dev/null
}

stop_process() {
    local pid_file=$1
    local name=$2

    if [ ! -f "$pid_file" ]; then
        log_warn "$name is not running (no PID file)"
        return 0
    fi

    local pid
    pid=$(cat "$pid_file")

    if ! kill -0 "$pid" 2>/dev/null; then
        log_warn "$name is not running (PID $pid stale)"
        rm -f "$pid_file"
        return 0
    fi

    log_info "Stopping $name (PID $pid)..."
    kill "$pid" 2>/dev/null || true

    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ $waited -lt 15 ]; do
        sleep 1
        waited=$((waited + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        log_warn "$name did not stop gracefully, force killing..."
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$pid_file"
    log_info "$name stopped"
}

is_yarn_mode() {
    local master_url="${1:-${SPARK_MASTER_URL:-}}"
    if [ -z "$master_url" ]; then
        return 1
    fi
    [[ "$master_url" =~ ^yarn ]]
}

resolve_deploy_mode() {
    local master_url="${1:-${SPARK_MASTER_URL:-}}"
    if is_yarn_mode "$master_url"; then
        echo "yarn"
    else
        echo "standalone"
    fi
}

ensure_dirs() {
    mkdir -p "$LOG_DIR" "$PID_DIR"
}

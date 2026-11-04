#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES_CONF="$SCRIPT_DIR/nodes.conf"
NODES_CONF_EXAMPLE="$SCRIPT_DIR/nodes.conf.example"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $*"; }

load_nodes_conf() {
    if [ -f "$NODES_CONF" ]; then
        source "$NODES_CONF"
        log_info "Loaded configuration from $NODES_CONF"
    elif [ -f "$NODES_CONF_EXAMPLE" ]; then
        log_warn "nodes.conf not found, falling back to nodes.conf.example"
        source "$NODES_CONF_EXAMPLE"
    else
        log_error "No nodes.conf or nodes.conf.example found in $SCRIPT_DIR"
        exit 1
    fi

    FRONTEND_NODES="${FRONTEND_NODES:-}"
    BACKEND_SERVER_NODE="${BACKEND_SERVER_NODE:-}"
    BACKEND_EXECUTOR_COUNT="${BACKEND_EXECUTOR_COUNT:-4}"
    METASTORE_NODE="${METASTORE_NODE:-}"
    SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[4]}"
    FRONTEND_PORT="${FRONTEND_PORT:-8080}"
    BACKEND_PORT="${BACKEND_PORT:-9090}"
    METASTORE_PORT="${METASTORE_PORT:-9091}"
    SSH_USER="${SSH_USER:-}"
    SSH_KEY="${SSH_KEY:-}"
    DEPLOY_PATH="${DEPLOY_PATH:-/opt/rawwise}"
    HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"
}

build_ssh_args() {
    local args=""
    if [ -n "$SSH_KEY" ]; then
        args="$args -i $SSH_KEY"
    fi
    args="$args -o StrictHostKeyChecking=no -o ConnectTimeout=10"
    echo "$args"
}

ssh_target() {
    local ssh_args
    ssh_args=$(build_ssh_args)
    local target
    if [ -n "$SSH_USER" ]; then
        target="$SSH_USER@$1"
    else
        target="$1"
    fi
    ssh $ssh_args "$target" "$2" 2>&1
}

remote_bin() {
    local node="$1"
    local script="$2"
    shift 2
    echo "$DEPLOY_PATH/bin/$script $*"
}

check_node_reachable() {
    local node="$1"
    local name="$2"
    if ! ssh_target "$node" "echo ok" >/dev/null 2>&1; then
        log_error "$name node ($node) is not reachable via SSH"
        return 1
    fi
    log_info "$name node ($node) is reachable"
}

start_metastore() {
    log_step "=== Starting Metastore ==="
    if [ -z "$METASTORE_NODE" ]; then
        log_warn "METASTORE_NODE is empty, skipping (backend will use embedded metastore)"
        return 0
    fi

    check_node_reachable "$METASTORE_NODE" "Metastore"

    local cmd
    cmd=$(remote_bin "$METASTORE_NODE" "metastore" "start" "--port" "$METASTORE_PORT")
    log_info "Executing on $METASTORE_NODE: $cmd"
    ssh_target "$METASTORE_NODE" "$cmd"

    local health_cmd
    health_cmd=$(remote_bin "$METASTORE_NODE" "metastore" "status")
    sleep 3
    local health_output
    health_output=$(ssh_target "$METASTORE_NODE" "$health_cmd" || true)
    log_info "Metastore status: $health_output"
}

start_backend() {
    log_step "=== Starting Backend Server ==="
    if [ -z "$BACKEND_SERVER_NODE" ]; then
        log_error "BACKEND_SERVER_NODE is required"
        exit 1
    fi

    check_node_reachable "$BACKEND_SERVER_NODE" "Backend Server"

    local cmd
    cmd=$(remote_bin "$BACKEND_SERVER_NODE" "backend" "start" \
        "--port" "$BACKEND_PORT" \
        "--master" "$SPARK_MASTER_URL")
    log_info "Executing on $BACKEND_SERVER_NODE: $cmd"
    ssh_target "$BACKEND_SERVER_NODE" "$cmd"

    sleep 3
    local health_cmd
    health_cmd=$(remote_bin "$BACKEND_SERVER_NODE" "backend" "status")
    local health_output
    health_output=$(ssh_target "$BACKEND_SERVER_NODE" "$health_cmd" || true)
    log_info "Backend status: $health_output"
}

start_frontends() {
    log_step "=== Starting Frontend Servers ==="
    if [ -z "$FRONTEND_NODES" ]; then
        log_error "FRONTEND_NODES is required"
        exit 1
    fi

    local backend_host="$BACKEND_SERVER_NODE"
    if [ -z "$backend_host" ]; then
        backend_host="localhost"
    fi

    for node in $FRONTEND_NODES; do
        log_info "Starting frontend on $node..."
        check_node_reachable "$node" "Frontend"

        local cmd
        cmd=$(remote_bin "$node" "frontend" "start" \
            "--port" "$FRONTEND_PORT" \
            "--backend-host" "$backend_host" \
            "--backend-port" "$BACKEND_PORT")
        log_info "Executing on $node: $cmd"
        ssh_target "$node" "$cmd"

        sleep 2
        local health_cmd
        health_cmd=$(remote_bin "$node" "frontend" "status")
        local health_output
        health_output=$(ssh_target "$node" "$health_cmd" || true)
        log_info "Frontend ($node) status: $health_output"
    done
}

load_nodes_conf

log_step "================================================"
log_step "  RawWise Cluster Start"
log_step "================================================"
log_info "Metastore:       ${METASTORE_NODE:-embedded}"
log_info "Backend Server:  $BACKEND_SERVER_NODE"
log_info "Frontend Nodes:  $FRONTEND_NODES"
log_info "Spark Master:    $SPARK_MASTER_URL"
log_info "Deploy Path:     $DEPLOY_PATH"
echo ""

start_metastore
echo ""
start_backend
echo ""
start_frontends

echo ""
log_step "================================================"
log_step "  Cluster Started Successfully"
log_step "================================================"
log_info "Frontend endpoints:"
for node in $FRONTEND_NODES; do
    log_info "  http://$node:$FRONTEND_PORT"
done
log_info ""
log_info "Use 'bin/deploy/cluster-status.sh' to check health"
log_info "Use 'bin/deploy/cluster-stop.sh' to stop the cluster"

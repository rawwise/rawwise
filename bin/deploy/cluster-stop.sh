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
    elif [ -f "$NODES_CONF_EXAMPLE" ]; then
        source "$NODES_CONF_EXAMPLE"
    else
        log_error "No nodes.conf or nodes.conf.example found in $SCRIPT_DIR"
        exit 1
    fi

    FRONTEND_NODES="${FRONTEND_NODES:-}"
    BACKEND_SERVER_NODE="${BACKEND_SERVER_NODE:-}"
    METASTORE_NODE="${METASTORE_NODE:-}"
    SSH_USER="${SSH_USER:-}"
    SSH_KEY="${SSH_KEY:-}"
    DEPLOY_PATH="${DEPLOY_PATH:-/opt/rawwise}"
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
    ssh $ssh_args "$target" "$2" 2>&1 || true
}

remote_bin() {
    local node="$1"
    local script="$2"
    echo "$DEPLOY_PATH/bin/$script"
}

stop_frontends() {
    log_step "=== Stopping Frontend Servers ==="
    if [ -z "$FRONTEND_NODES" ]; then
        log_warn "No FRONTEND_NODES configured, skipping"
        return 0
    fi

    for node in $FRONTEND_NODES; do
        log_info "Stopping frontend on $node..."
        local cmd
        cmd="$(remote_bin "$node" "frontend") stop"
        ssh_target "$node" "$cmd"
        log_info "Frontend on $node stopped"
    done
}

stop_backend() {
    log_step "=== Stopping Backend Server ==="
    if [ -z "$BACKEND_SERVER_NODE" ]; then
        log_warn "No BACKEND_SERVER_NODE configured, skipping"
        return 0
    fi

    log_info "Stopping backend on $BACKEND_SERVER_NODE..."
    local cmd
    cmd="$(remote_bin "$BACKEND_SERVER_NODE" "backend") stop"
    ssh_target "$BACKEND_SERVER_NODE" "$cmd"
    log_info "Backend on $BACKEND_SERVER_NODE stopped"
}

stop_metastore() {
    log_step "=== Stopping Metastore ==="
    if [ -z "$METASTORE_NODE" ]; then
        log_warn "No METASTORE_NODE configured (embedded metastore), skipping"
        return 0
    fi

    log_info "Stopping metastore on $METASTORE_NODE..."
    local cmd
    cmd="$(remote_bin "$METASTORE_NODE" "metastore") stop"
    ssh_target "$METASTORE_NODE" "$cmd"
    log_info "Metastore on $METASTORE_NODE stopped"
}

load_nodes_conf

log_step "================================================"
log_step "  RawWise Cluster Stop"
log_step "================================================"
log_info "Stopping in reverse dependency order: Frontend -> Backend -> Metastore"
echo ""

stop_frontends
echo ""
stop_backend
echo ""
stop_metastore

echo ""
log_step "================================================"
log_step "  Cluster Stopped"
log_step "================================================"

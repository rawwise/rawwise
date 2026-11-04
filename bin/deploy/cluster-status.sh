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
    FRONTEND_PORT="${FRONTEND_PORT:-8080}"
    BACKEND_PORT="${BACKEND_PORT:-9090}"
    METASTORE_PORT="${METASTORE_PORT:-9091}"
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
    echo "$DEPLOY_PATH/bin/$1"
}

check_ssh_connectivity() {
    local node="$1"
    if ssh_target "$node" "echo ok" 2>/dev/null | grep -q ok; then
        echo -e "${GREEN}REACHABLE${NC}"
    else
        echo -e "${RED}UNREACHABLE${NC}"
    fi
}

check_process_status() {
    local node="$1"
    local component="$2"
    local cmd
    cmd="$(remote_bin "$component") status"

    local output
    output=$(ssh_target "$node" "$cmd" 2>/dev/null || true)
    if echo "$output" | grep -qi "RUNNING"; then
        echo -e "${GREEN}RUNNING${NC}"
    elif echo "$output" | grep -qi "STOPPED"; then
        echo -e "${RED}STOPPED${NC}"
    else
        echo -e "${YELLOW}UNKNOWN${NC}"
    fi
}

check_frontend_health() {
    local node="$1"
    local port="$2"

    local curl_output
    curl_output=$(ssh_target "$node" "curl -sf -o /dev/null -w '%{http_code}' http://localhost:$port/health 2>/dev/null" || true)

    if [ "$curl_output" = "200" ]; then
        echo -e "${GREEN}HEALTHY${NC} (HTTP $curl_output)"
    elif [ -n "$curl_output" ]; then
        echo -e "${YELLOW}DEGRADED${NC} (HTTP $curl_output)"
    else
        echo -e "${RED}NO HEALTH RESPONSE${NC}"
    fi
}

check_port_listening() {
    local node="$1"
    local port="$2"

    local output
    output=$(ssh_target "$node" "lsof -i :$port 2>/dev/null || ss -tlnp 2>/dev/null | grep :$port || true" 2>/dev/null || true)

    if [ -n "$output" ]; then
        echo -e "${GREEN}LISTENING${NC}"
    else
        echo -e "${RED}NOT LISTENING${NC}"
    fi
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [--json]

Check health status of all RawWise cluster components.

Options:
  --json    Output status in JSON format

EOF
}

check_status() {
    echo ""
    echo "================================================"
    echo "  RawWise Cluster Health Check"
    echo "================================================"
    echo ""

    local overall_ok=true

    echo "--- Metastore ---"
    if [ -n "$METASTORE_NODE" ]; then
        local ssh_stat
        ssh_stat=$(check_ssh_connectivity "$METASTORE_NODE")
        echo -e "  SSH:       $ssh_stat"
        if echo "$ssh_stat" | grep -q "REACHABLE"; then
            local proc_stat
            proc_stat=$(check_process_status "$METASTORE_NODE" "metastore")
            echo -e "  Process:   $proc_stat"
            local port_stat
            port_stat=$(check_port_listening "$METASTORE_NODE" "$METASTORE_PORT")
            echo -e "  Port $METASTORE_PORT: $port_stat"
            if ! echo "$proc_stat" | grep -q "RUNNING"; then
                overall_ok=false
            fi
        else
            overall_ok=false
        fi
    else
        echo -e "  Metastore: ${YELLOW}EMBEDDED (runs inside Backend Server)${NC}"
    fi
    echo ""

    echo "--- Backend Server ---"
    if [ -n "$BACKEND_SERVER_NODE" ]; then
        local ssh_stat
        ssh_stat=$(check_ssh_connectivity "$BACKEND_SERVER_NODE")
        echo -e "  SSH:       $ssh_stat"
        if echo "$ssh_stat" | grep -q "REACHABLE"; then
            local proc_stat
            proc_stat=$(check_process_status "$BACKEND_SERVER_NODE" "backend")
            echo -e "  Process:   $proc_stat"
            local port_stat
            port_stat=$(check_port_listening "$BACKEND_SERVER_NODE" "$BACKEND_PORT")
            echo -e "  Port $BACKEND_PORT: $port_stat"
            if ! echo "$proc_stat" | grep -q "RUNNING"; then
                overall_ok=false
            fi
        else
            overall_ok=false
        fi
    else
        echo -e "  Backend: ${RED}NOT CONFIGURED${NC}"
        overall_ok=false
    fi
    echo ""

    echo "--- Frontend Servers ---"
    if [ -n "$FRONTEND_NODES" ]; then
        for node in $FRONTEND_NODES; do
            echo "  [$node]"
            local ssh_stat
            ssh_stat=$(check_ssh_connectivity "$node")
            echo -e "    SSH:       $ssh_stat"
            if echo "$ssh_stat" | grep -q "REACHABLE"; then
                local proc_stat
                proc_stat=$(check_process_status "$node" "frontend")
                echo -e "    Process:   $proc_stat"
                local port_stat
                port_stat=$(check_port_listening "$node" "$FRONTEND_PORT")
                echo -e "    Port $FRONTEND_PORT: $port_stat"
                local health_stat
                health_stat=$(check_frontend_health "$node" "$FRONTEND_PORT")
                echo -e "    Health:    $health_stat"
                if ! echo "$proc_stat" | grep -q "RUNNING"; then
                    overall_ok=false
                fi
            else
                overall_ok=false
            fi
            echo ""
        done
    else
        echo -e "  Frontend: ${RED}NOT CONFIGURED${NC}"
        overall_ok=false
    fi

    if $overall_ok; then
        echo -e "${GREEN}Overall: All components healthy${NC}"
    else
        echo -e "${RED}Overall: Some components are unhealthy${NC}"
    fi
    echo ""

    if $overall_ok; then
        return 0
    else
        return 1
    fi
}

check_status_json() {
    load_nodes_conf

    local metastore_status="embedded"
    local metastore_ssh="n/a"
    local backend_status="stopped"
    local backend_ssh="n/a"
    local frontends_json="[]"

    if [ -n "$METASTORE_NODE" ]; then
        metastore_ssh=$(ssh_target "$METASTORE_NODE" "echo ok" 2>/dev/null | grep -q ok && echo "reachable" || echo "unreachable")
        if [ "$metastore_ssh" = "reachable" ]; then
            metastore_status=$(ssh_target "$METASTORE_NODE" "$(remote_bin metastore) status" 2>/dev/null | grep -qi running && echo "running" || echo "stopped")
        else
            metastore_status="unreachable"
        fi
    fi

    if [ -n "$BACKEND_SERVER_NODE" ]; then
        backend_ssh=$(ssh_target "$BACKEND_SERVER_NODE" "echo ok" 2>/dev/null | grep -q ok && echo "reachable" || echo "unreachable")
        if [ "$backend_ssh" = "reachable" ]; then
            backend_status=$(ssh_target "$BACKEND_SERVER_NODE" "$(remote_bin backend) status" 2>/dev/null | grep -qi running && echo "running" || echo "stopped")
        else
            backend_status="unreachable"
        fi
    fi

    if [ -n "$FRONTEND_NODES" ]; then
        local entries=""
        for node in $FRONTEND_NODES; do
            local f_ssh
            f_ssh=$(ssh_target "$node" "echo ok" 2>/dev/null | grep -q ok && echo "reachable" || echo "unreachable")
            local f_status="unknown"
            if [ "$f_ssh" = "reachable" ]; then
                f_status=$(ssh_target "$node" "$(remote_bin frontend) status" 2>/dev/null | grep -qi running && echo "running" || echo "stopped")
            fi
            if [ -n "$entries" ]; then
                entries="$entries,"
            fi
            entries="$entries{\"node\":\"$node\",\"ssh\":\"$f_ssh\",\"status\":\"$f_status\"}"
        done
        frontends_json="[$entries]"
    fi

    cat <<JSONEOF
{
  "metastore": {
    "node": "${METASTORE_NODE:-embedded}",
    "ssh": "$metastore_ssh",
    "status": "$metastore_status"
  },
  "backend": {
    "node": "${BACKEND_SERVER_NODE:-}",
    "ssh": "$backend_ssh",
    "status": "$backend_status"
  },
  "frontends": $frontends_json
}
JSONEOF
}

load_nodes_conf

case "${1:-}" in
    --json)
        check_status_json
        ;;
    *)
        check_status
        ;;
esac

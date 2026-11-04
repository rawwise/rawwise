#!/bin/bash
# RawWise 进程状态速查 — 单进程 Embedded 模式
# 用法: bash bin/rawwise-status.sh [--profile=dogfood|benchmark|test|all]
set -e

RED=$'\033[31m'
GREEN=$'\033[32m'
YELLOW=$'\033[33m'
BLUE=$'\033[34m'
GRAY=$'\033[90m'
RESET=$'\033[0m'
BOLD=$'\033[1m'

# ====== Profile 解析 ======
PROFILE="${RAWWISE_PROFILE:-all}"
for arg in "$@"; do
    case "$arg" in
        --profile=*) PROFILE="${arg#*=}" ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RAW_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# profile 信息表
profile_info() {
    case "$1" in
        dogfood)   echo "8080 9001 $HOME/.rawwise/data $RAW_ROOT/run/rawwise-dogfood.pid 常驻生产 dogfooding" ;;
        benchmark) echo "8081 9002 /tmp/rawwise-bench $RAW_ROOT/run/rawwise-benchmark.pid 一次性压测 benchmark" ;;
        test)      echo "8082 9003 /tmp/rawwise-test $RAW_ROOT/run/rawwise-test.pid 临时验证 test" ;;
        *) return 1 ;;
    esac
}

print_header() {
    printf "${BOLD}%-12s %-8s %-10s %-8s %-8s %-10s %s${RESET}\n" \
        "Profile" "PID" "Port" "Mem(MB)" "CPU%" "Uptime" "Note"
    echo "─────────────────────────────────────────────────────────────────────────────"
}

print_row() {
    local name="$1" pid="$2" port="$3" mem="$4" cpu="$5" uptime="$6" note="$7"
    local color="$GREEN"
    [ "$pid" = "—" ] && color="$RED"
    printf "${color}%-12s${RESET} %-8s %-10s %-8s %-8s %-10s ${GRAY}%s${RESET}\n" \
        "$name" "$pid" "$port" "$mem" "$cpu" "$uptime" "$note"
}

get_stats() {
    local pid="$1" port="$2"
    if [ -z "$pid" ]; then
        echo "— — — —"
        return
    fi
    local stats
    stats=$(ps -o rss=,%cpu=,etime= -p "$pid" 2>/dev/null | awk '{print int($1/1024), $2, $3}')
    [ -z "$stats" ] && { echo "— — — —"; return; }
    local mem cpu uptime
    read mem cpu uptime <<< "$stats"
    local port_status="—"
    if [ -n "$port" ]; then
        if lsof -ti:"$port" 2>/dev/null | grep -q .; then
            port_status="$port"
        else
            port_status="${port}✗"
        fi
    fi
    echo "$port_status $mem ${cpu}% $uptime"
}

show_profile() {
    local p="$1"
    local info
    info=$(profile_info "$p") || return
    local fport mport data_dir pid_file desc
    read fport mport data_dir pid_file desc <<< "$info"

    local pid=""
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file" 2>/dev/null || echo "")
    fi
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        pid=$(lsof -ti:"$fport" 2>/dev/null | head -1 || echo "")
    fi

    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        read port mem cpu uptime <<< "$(get_stats "$pid" "$fport")"
        print_row "$p" "$pid" "$port" "$mem" "$cpu" "$uptime" "$desc"
    else
        print_row "$p" "—" "$fport" "—" "—" "—" "${RED}DOWN${RESET}"
    fi
}

echo ""
echo "${BOLD}RawWise 服务状态${RESET}"

if [ "$PROFILE" = "all" ]; then
    echo "  (showing all profiles)"
    echo ""
    print_header
    for p in dogfood benchmark test; do
        show_profile "$p"
    done
else
    local_info=$(profile_info "$PROFILE" 2>/dev/null) || {
        echo "ERROR: Unknown profile: $PROFILE (use dogfood|benchmark|test|all)"
        exit 1
    }
    local fport mport data_dir pid_file desc
    read fport mport data_dir pid_file desc <<< "$local_info"
    echo "  (profile: ${BLUE}$PROFILE${RESET} — $desc)"
    echo "  (data: ${BLUE}$data_dir${RESET})"
    echo ""
    print_header
    show_profile "$PROFILE"
fi

echo ""
echo "${BOLD}健康检查:${RESET}"

# 健康检查的目标端口
if [ "$PROFILE" = "all" ]; then
    HEALTH_PORTS="8080 8081 8082"
    HEALTH_NAMES="dogfood benchmark test"
else
    info=$(profile_info "$PROFILE") || exit 1
    read HEALTH_PORTS mport data_dir pid_file desc <<< "$info"
    HEALTH_NAMES="$PROFILE"
fi

idx=0
for port in $HEALTH_PORTS; do
    name=$(echo "$HEALTH_NAMES" | cut -d' ' -f$((idx+1)))
    idx=$((idx+1))
    http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://localhost:$port/api/query" -X POST -H "Content-Type: application/json" -d '{"sql":"SELECT 1"}' 2>/dev/null || echo "000")
    case "$http_code" in
        200|400) echo "  ${GREEN}✓${RESET} [$name] Frontend HTTP :$port → $http_code (responding)" ;;
        401)     echo "  ${YELLOW}⚠${RESET} [$name] Frontend HTTP :$port → 401 (auth on, expected)" ;;
        000)     echo "  ${RED}✗${RESET} [$name] Frontend HTTP :$port → connection failed" ;;
        *)       echo "  ${YELLOW}⚠${RESET} [$name] Frontend HTTP :$port → $http_code" ;;
    esac
done

echo ""
echo "${GRAY}操作:${RESET}"
echo "  启动: bash bin/rawwise-start.sh --profile=<dogfood|benchmark|test>"
echo "  停止: bash bin/rawwise-stop.sh --profile=<name>"
echo "  重启: bash bin/rawwise-restart.sh --profile=<name>"
echo "  清空: bash bin/rawwise-clean.sh --profile=<name>"
echo ""

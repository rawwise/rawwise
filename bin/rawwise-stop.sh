#!/bin/bash
# RawWise 停止脚本 — 单进程 Embedded 模式
# 用法: bash bin/rawwise-stop.sh [--profile=dogfood|benchmark|test]
set +e

# ====== Profile 解析 ======
PROFILE="${RAWWISE_PROFILE:-dogfood}"
for arg in "$@"; do
    case "$arg" in
        --profile=*) PROFILE="${arg#*=}" ;;
    esac
done

RAW_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "$PROFILE" in
    dogfood)
        FRONTEND_PORT=8080; METASTORE_PORT=9001
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        ;;
    benchmark)
        FRONTEND_PORT=8081; METASTORE_PORT=9002
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        ;;
    test)
        FRONTEND_PORT=8082; METASTORE_PORT=9003
        PID_FILE="$RAW_ROOT/run/rawwise-$PROFILE.pid"
        ;;
    *)
        echo "ERROR: Unknown profile: $PROFILE (use dogfood|benchmark|test)"
        exit 1
        ;;
esac

GRACEFUL_TIMEOUT=30
KILL_TIMEOUT=5

echo "==> Stopping RawWise (profile=$PROFILE, Embedded mode)..."

# 1. 优先用 PID 文件定位进程（精准）
PID=""
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
fi

# 2. PID 文件无效时，回退到 pgrep（兜底）
if [ -z "$PID" ] || ! kill -0 "$PID" 2>/dev/null; then
    PID=$(pgrep -f "io.github.rawwise.frontend.server.Main" 2>/dev/null | head -1 || echo "")
fi

if [ -z "$PID" ]; then
    echo "   RawWise (profile=$PROFILE) not running"
    rm -f "$PID_FILE" 2>/dev/null
    # 仍然检查端口是否被占用（孤儿进程）
    PORT_PID=$(lsof -ti:"$FRONTEND_PORT" 2>/dev/null || true)
    if [ -n "$PORT_PID" ]; then
        echo "   Port $FRONTEND_PORT still occupied by PID=$PORT_PID (orphan), force killing"
        kill -9 "$PORT_PID" 2>/dev/null || true
        sleep 2
    fi
    echo "==> Done."
    exit 0
fi

echo "   Target PID=$PID (from ${PID_FILE:-pgrep fallback})"

# 3. 优雅关闭 (SIGTERM)
kill "$PID" 2>/dev/null
echo "   SIGTERM sent, waiting up to ${GRACEFUL_TIMEOUT}s for graceful shutdown..."

KILLED_GRACEFULLY=0
for i in $(seq 1 $GRACEFUL_TIMEOUT); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "   Process exited gracefully after ${i}s"
        KILLED_GRACEFULLY=1
        break
    fi
    sleep 1
done

# 4. SIGKILL 残留进程（仅当优雅关闭失败）
if [ "$KILLED_GRACEFULLY" = "0" ]; then
    echo "   WARN: Process did not exit after ${GRACEFUL_TIMEOUT}s, sending SIGKILL"
    echo "   NOTE: LMDB lock may not be released cleanly. If next start fails with lock error,"
    echo "         verify no process holds the lock: lsof +D ~/.rawwise/data/warehouse"
    kill -9 "$PID" 2>/dev/null
    sleep 2
fi

# 5. 清理 PID 文件
rm -f "$PID_FILE" 2>/dev/null

# 6. 端口占用兜底检查
for PORT in $FRONTEND_PORT $METASTORE_PORT; do
    PORT_PID=$(lsof -ti:"$PORT" 2>/dev/null || true)
    if [ -n "$PORT_PID" ]; then
        echo "   Port $PORT still occupied by PID=$PORT_PID, force killing"
        kill -9 "$PORT_PID" 2>/dev/null || true
        sleep 1
    fi
done

# 7. 最终确认
ALL_FREE=1
for PORT in $FRONTEND_PORT $METASTORE_PORT; do
    PORT_PID=$(lsof -ti:"$PORT" 2>/dev/null || true)
    if [ -n "$PORT_PID" ]; then
        echo "   ⚠️  Port $PORT STILL OCCUPIED PID=$PORT_PID"
        ALL_FREE=0
    fi
done

if [ "$ALL_FREE" = "1" ]; then
    echo "==> Done. All ports released (profile=$PROFILE)."
else
    echo "==> Done with warnings (profile=$PROFILE). Some ports may be in TIME_WAIT."
fi

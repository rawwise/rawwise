#!/bin/bash
# RawWise 数据清理脚本 — 删除指定 profile 的所有持久化数据
# 用法: bash bin/rawwise-clean.sh [--profile=dogfood|benchmark|test]
#
# 注意：
#   - benchmark/test profile 的数据可以随时清除（临时数据）
#   - dogfood profile 的数据包含知识库（dev_dialogs / project_docs / dev_decisions），清除后需重新 bootstrap
#   - 推荐优先使用 bin/rawwise-drop-all-tables.sh（通过 SQL DROP TABLE，更安全）
set -e

# ====== Profile 解析 ======
PROFILE="${RAWWISE_PROFILE:-dogfood}"
for arg in "$@"; do
    case "$arg" in
        --profile=*) PROFILE="${arg#*=}" ;;
    esac
done

case "$PROFILE" in
    dogfood)
        DATA_DIR="$HOME/.rawwise/data"
        DANGER_MSG="⚠️  这将删除 dogfooding 知识库（dev_dialogs / project_docs / dev_decisions）！"
        ;;
    benchmark)
        DATA_DIR="/tmp/rawwise-bench"
        DANGER_MSG=""
        ;;
    test)
        DATA_DIR="/tmp/rawwise-test"
        DANGER_MSG=""
        ;;
    *)
        echo "ERROR: Unknown profile: $PROFILE (use dogfood|benchmark|test)"
        exit 1
        ;;
esac

echo "==> This will DELETE all RawWise data for profile '$PROFILE' in:"
echo "    $DATA_DIR"
[ -n "$DANGER_MSG" ] && echo "    $DANGER_MSG"
echo ""

if [ ! -d "$DATA_DIR" ]; then
    echo "    Data directory does not exist. Nothing to clean."
    exit 0
fi

echo "    Contents:"
du -sh "$DATA_DIR"/* 2>/dev/null || true
echo ""

# 检查 RawWise 该 profile 是否在运行
PROFILE_PORT=""
case "$PROFILE" in
    dogfood)   PROFILE_PORT=8080 ;;
    benchmark) PROFILE_PORT=8081 ;;
    test)      PROFILE_PORT=8082 ;;
esac

if lsof -ti:"$PROFILE_PORT" 2>/dev/null | grep -q .; then
    echo "ERROR: RawWise profile '$PROFILE' is still running (port $PROFILE_PORT occupied)."
    echo "       Stop it first: bash bin/rawwise-stop.sh --profile=$PROFILE"
    exit 1
fi

echo "    💡 推荐替代方案（不需要停服务）："
echo "       bash bin/rawwise-drop-all-tables.sh --profile=$PROFILE"
echo "       （通过 SQL DROP TABLE 清理，保留知识库 schema）"
echo ""

read -p "    Confirm FULL DELETE of $DATA_DIR? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "    Cancelled."
    exit 0
fi

rm -rf "$DATA_DIR"
echo "==> Data cleaned for profile=$PROFILE. Next restart will start fresh."
echo "    如需重建知识库: bash bin/rawwise-bootstrap-knowledge.sh"

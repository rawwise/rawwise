#!/bin/bash
# RawWise 重启脚本：stop → (可选部署新jar) → start
# 用法: bash bin/rawwise-restart.sh [--profile=dogfood|benchmark|test] [--no-package]
#   --no-package  跳过 jar 部署（默认会部署 target → lib）
set +e

RAW_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ====== Profile 解析 ======
PROFILE="${RAWWISE_PROFILE:-dogfood}"
DEPLOY_JAR=1
for arg in "$@"; do
    case "$arg" in
        --profile=*) PROFILE="${arg#*=}" ;;
        --no-package) DEPLOY_JAR=0 ;;
    esac
done

echo "==> Restarting RawWise (profile=$PROFILE)..."

# 1. Stop
bash "$RAW_ROOT/bin/rawwise-stop.sh" --profile="$PROFILE"

# 2. 部署最新 jar（target → lib）
if [ "$DEPLOY_JAR" = "1" ]; then
    TARGET_JAR="$RAW_ROOT/rawwise-core/target/rawwise-core-0.0.1.jar"
    LIB_JAR="$RAW_ROOT/lib/rawwise-core-0.0.1.jar"
    if [ -f "$TARGET_JAR" ]; then
        if [ ! -f "$LIB_JAR" ] || ! cmp -s "$TARGET_JAR" "$LIB_JAR"; then
            mkdir -p "$RAW_ROOT/lib"
            cp "$TARGET_JAR" "$LIB_JAR"
            echo "   Deployed new jar: $(basename "$TARGET_JAR") → lib/"
        else
            echo "   Jar already up-to-date, skipping copy."
        fi
    else
        echo "   WARN: target jar not found ($TARGET_JAR), run 'bash bin/rawwise-package.sh' first"
    fi
fi

# 3. Start
bash "$RAW_ROOT/bin/rawwise-start.sh" --profile="$PROFILE"

echo "==> Restart complete (profile=$PROFILE)."

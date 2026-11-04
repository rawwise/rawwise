#!/bin/bash
# RawWise 打包脚本 — 编译 + 打包 JAR + 计算 classpath，部署到 lib/
# 用法: bash bin/rawwise-package.sh [--skip-tests]
#
# 此脚本用于更新本地 dogfooding 环境的 JAR 包。打包后执行：
#   bash bin/rawwise-restart.sh --profile=dogfood
# 即可重启 dogfooding 生产环境加载新 JAR。
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKIP_TESTS="${1:-}"

cd "$ROOT"

echo "==> [1/4] Compiling rawwise-core..."
mvn compile -T1 -pl rawwise-core -q

echo "==> [2/4] Packaging rawwise-core JAR..."
if [ "$SKIP_TESTS" = "--skip-tests" ]; then
    mvn package -pl rawwise-core -Dmaven.test.skip=true -T1 -q
else
    mvn package -pl rawwise-core -DskipTests -T1 -q
fi

echo "==> [3/4] Computing classpath..."
mkdir -p /tmp/rawwise lib
mvn dependency:build-classpath -pl rawwise-core -Dmdep.outputFile=/tmp/rawwise/cp-full.txt -q

echo "==> [4/4] Deploying to lib/..."
mkdir -p "$ROOT/lib"

# 复制 JAR
TARGET_JAR="$ROOT/rawwise-core/target/rawwise-core-0.0.1.jar"
LIB_JAR="$ROOT/lib/rawwise-core-0.0.1.jar"
if [ -f "$TARGET_JAR" ]; then
    if [ ! -f "$LIB_JAR" ] || ! cmp -s "$TARGET_JAR" "$LIB_JAR"; then
        cp "$TARGET_JAR" "$LIB_JAR"
        echo "    JAR deployed: $(basename "$TARGET_JAR") → lib/"
    else
        echo "    JAR already up-to-date (no change)."
    fi
else
    echo "    ERROR: target JAR not found: $TARGET_JAR"
    exit 1
fi

# 复制 classpath
cp /tmp/rawwise/cp-full.txt "$ROOT/lib/classpath.txt"
echo "    Classpath deployed: lib/classpath.txt"

# 同步部署到 deploy/ 目录（如果存在）
DEPLOY_DIR="$ROOT/deploy"
if [ -d "$DEPLOY_DIR" ]; then
    mkdir -p "$DEPLOY_DIR/lib"
    cp "$LIB_JAR" "$DEPLOY_DIR/lib/"
    cp "$ROOT/lib/classpath.txt" "$DEPLOY_DIR/lib/"
    echo "    Also synced to deploy/lib/"
fi

echo ""
echo "==> Package complete. JAR ready at lib/rawwise-core-0.0.1.jar"
echo ""
echo "    Next steps:"
echo "      重启 dogfooding:  bash bin/rawwise-restart.sh --profile=dogfood"
echo "      重启 benchmark:   bash bin/rawwise-restart.sh --profile=benchmark"
echo "      查看状态:         bash bin/rawwise-status.sh --profile=all"

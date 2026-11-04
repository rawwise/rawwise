#!/bin/bash
# RawWise 打包脚本 — 生成可部署的二进制包到 deploy/ 目录
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY_DIR="$ROOT/deploy"

echo "==> Compiling..."
cd "$ROOT"
mvn compile -T1 -pl rawwise-core -q

echo "==> Packaging..."
mvn package -pl rawwise-core -Dmaven.test.skip=true -T1 -q

echo "==> Computing classpath..."
mkdir -p /tmp/rawwise-pkg
mvn dependency:build-classpath -pl rawwise-core -Dmdep.outputFile=/tmp/rawwise-pkg/cp-full.txt -q

echo "==> Assembling deployment package to $DEPLOY_DIR ..."
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"/{bin,lib,logs}

# Copy JARs
cp rawwise-core/target/rawwise-core-*.jar "$DEPLOY_DIR/lib/"
cp rawwise-core/target/original-rawwise-core-*.jar "$DEPLOY_DIR/lib/" 2>/dev/null || true

# Copy classpath file
cp /tmp/rawwise-pkg/cp-full.txt "$DEPLOY_DIR/lib/classpath.txt"

# Copy scripts
cp bin/rawwise-start.sh "$DEPLOY_DIR/bin/"
cp bin/rawwise-stop.sh "$DEPLOY_DIR/bin/"
cp bin/rawwise-clean.sh "$DEPLOY_DIR/bin/"
cp bin/rawwise-bootstrap-knowledge.sh "$DEPLOY_DIR/bin/"
chmod +x "$DEPLOY_DIR/bin/"*.sh

echo "==> Package assembled at $DEPLOY_DIR"
echo "    Start:  $DEPLOY_DIR/bin/rawwise-start.sh"
echo "    Stop:   $DEPLOY_DIR/bin/rawwise-stop.sh"
echo "    Logs:   $DEPLOY_DIR/logs/"

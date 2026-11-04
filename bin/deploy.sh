#!/bin/bash
set -e

# =============================================================
# RawWise 一键部署脚本
# 用法:
#   bash bin/deploy.sh init      # 初始化配置文件
#   bash bin/deploy.sh package   # 编译打包
#   bash bin/deploy.sh start     # 启动服务 (standalone 或 cluster)
#   bash bin/deploy.sh stop      # 停止服务
#   bash bin/deploy.sh status    # 查看状态
#   bash bin/deploy.sh restart   # 重启
#   bash bin/deploy.sh clean     # 清理数据
# =============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RAW_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONF_FILE="${RAWWISE_DEPLOY_CONF:-$RAW_ROOT/deploy/deploy.conf}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*" >&2; }

# ====== 加载配置 ======
load_config() {
    if [ ! -f "$CONF_FILE" ]; then
        err "配置文件不存在: $CONF_FILE"
        err "请先运行: bash bin/deploy.sh init"
        exit 1
    fi
    set -a
    # shellcheck disable=SC1090
    . "$CONF_FILE"
    set +a

    DEPLOY_MODE="${DEPLOY_MODE:-standalone}"
    DATA_DIR="${DATA_DIR:-$HOME/.rawwise/data}"
    FRONTEND_PORT="${FRONTEND_PORT:-8080}"
    METASTORE_GRPC_PORT="${METASTORE_GRPC_PORT:-9001}"
    BACKEND_SERVER_GRPC_PORT="${BACKEND_SERVER_GRPC_PORT:-9090}"

    if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        export PATH="$JAVA_HOME/bin:$PATH"
    fi

    if ! command -v java >/dev/null 2>&1; then
        err "Java 未安装或不在 PATH 中。请安装 Java 21+。"
        exit 1
    fi
    JAVA_VERSION=$(java -version 2>&1 | head -1 | sed 's/.*"\([0-9]*\).*/\1/')
    if [ "$JAVA_VERSION" -lt 21 ] 2>/dev/null; then
        err "需要 Java 21+, 当前版本: $(java -version 2>&1 | head -1)"
        exit 1
    fi

    if [ "$DEPLOY_MODE" = "cluster" ]; then
        if [ -n "${SSH_PASSWORD:-}" ] && [ -z "${SSH_KEY:-}" ]; then
            if ! command -v sshpass >/dev/null 2>&1; then
                err "集群密码认证需要 sshpass。请安装: apt install sshpass (或使用 SSH_KEY 代替)"
                exit 1
            fi
        fi
        if ! command -v ssh >/dev/null 2>&1; then
            err "集群模式需要 ssh 客户端。请安装 openssh-client。"
            exit 1
        fi
    fi
}

# ====== 解析 JAR 版本 ======
resolve_jar_name() {
    local pom="$RAW_ROOT/rawwise-core/pom.xml"
    if [ -f "$pom" ]; then
        local ver
        ver=$(grep -m1 '<version>' "$pom" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
        echo "rawwise-core-${ver}.jar"
    else
        echo "rawwise-core-0.0.1.jar"
    fi
}

# ====== 检查/构建 classpath ======
ensure_classpath() {
    local jar_name
    jar_name=$(resolve_jar_name)
    local cp_file="$RAW_ROOT/lib/classpath.txt"
    local jar_path="$RAW_ROOT/lib/$jar_name"

    if [ -f "$cp_file" ] && [ -f "$jar_path" ]; then
        return 0
    fi

    log "JAR 或 classpath 不存在，开始编译打包..."
    do_package
}

# ====== package 命令 ======
do_package() {
    log "编译打包 RawWise..."
    cd "$RAW_ROOT"

    if ! mvn package -pl rawwise-core -Dmaven.test.skip=true -q 2>&1; then
        err "编译失败"
        exit 1
    fi

    local jar_name
    jar_name=$(resolve_jar_name)
    mkdir -p "$RAW_ROOT/lib"

    cp "$RAW_ROOT/rawwise-core/target/$jar_name" "$RAW_ROOT/lib/"

    log "计算 classpath..."
    mvn dependency:build-classpath -pl rawwise-core -Dmdep.outputFile="$RAW_ROOT/lib/classpath.txt" -q 2>/dev/null || true

    if [ ! -s "$RAW_ROOT/lib/classpath.txt" ]; then
        warn "classpath 为空，尝试从 target 读取..."
        if [ -f "$RAW_ROOT/rawwise-core/target/classes" ]; then
            find "$RAW_ROOT/rawwise-core/target" -name "*.jar" 2>/dev/null | tr '\n' ':' > "$RAW_ROOT/lib/classpath.txt"
        fi
    fi

    log "打包完成: lib/$jar_name + lib/classpath.txt"
}

# ====== standalone 启动 ======
start_standalone() {
    log "启动 Standalone 模式 (Embedded, 单 JVM)..."

    ensure_classpath

    export RAWWISE_DATA_DIR="$DATA_DIR"
    export RAWWISE_FRONTEND_PORT="$FRONTEND_PORT"
    export RAWWISE_METASTORE_PORT="$METASTORE_GRPC_PORT"
    export RAWWISE_EMBEDDED_HEAP="${EMBEDDED_HEAP:-6g}"
    export RAWWISE_MAX_DIRECT_MEMORY="${MAX_DIRECT_MEMORY:-2g}"
    export RAWWISE_STORAGE_LMDB_INIT_MB="${STORAGE_LMDB_INIT_MB:-256}"
    export RAWWISE_STORAGE_LMDB_MAX_MB="${STORAGE_LMDB_MAX_MB:-4096}"
    export RAWWISE_EMBEDDING_TYPE="${EMBEDDING_TYPE:-noop}"
    export RAWWISE_EMBEDDING_ENDPOINT="${EMBEDDING_ENDPOINT:-}"
    export RAWWISE_EMBEDDING_MODEL="${EMBEDDING_MODEL:-}"
    export RAWWISE_EMBEDDING_API_KEY="${EMBEDDING_API_KEY:-}"
    export RAWWISE_EMBEDDING_DIMENSION="${EMBEDDING_DIMENSION:-1024}"
    export RAWWISE_SUMMARIZATION_TYPE="${SUMMARIZATION_TYPE:-noop}"
    export RAWWISE_SUMMARIZATION_ENDPOINT="${SUMMARIZATION_ENDPOINT:-}"
    export RAWWISE_SUMMARIZATION_MODEL="${SUMMARIZATION_MODEL:-}"
    export RAWWISE_SUMMARIZATION_API_KEY="${SUMMARIZATION_API_KEY:-}"
    export RAWWISE_RATE_LIMIT_PER_IP="${RATE_LIMIT_PER_IP:-10000}"
    export RAWWISE_RATE_LIMIT_GLOBAL="${RATE_LIMIT_GLOBAL:-50000}"

    bash "$SCRIPT_DIR/rawwise-start.sh" --profile=dogfood
}

# ====== cluster SSH 辅助 ======
ssh_cmd() {
    local host="$1"; shift
    local ssh_opts="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
    if [ -n "${SSH_KEY:-}" ] && [ -f "$SSH_KEY" ]; then
        ssh $ssh_opts -i "$SSH_KEY" "${SSH_USER:-rawwise}@$host" "$@"
    elif [ -n "${SSH_PASSWORD:-}" ]; then
        sshpass -p "$SSH_PASSWORD" ssh $ssh_opts "${SSH_USER:-rawwise}@$host" "$@"
    else
        ssh $ssh_opts "${SSH_USER:-rawwise}@$host" "$@"
    fi
}

scp_file() {
    local src="$1" dst_host="$2" dst_path="$3"
    local ssh_opts="-o StrictHostKeyChecking=no -o ConnectTimeout=10"
    if [ -n "${SSH_KEY:-}" ] && [ -f "$SSH_KEY" ]; then
        scp $ssh_opts -i "$SSH_KEY" "$src" "${SSH_USER:-rawwise}@$dst_host:$dst_path"
    elif [ -n "${SSH_PASSWORD:-}" ]; then
        sshpass -p "$SSH_PASSWORD" scp $ssh_opts "$src" "${SSH_USER:-rawwise}@$dst_host:$dst_path"
    else
        scp $ssh_opts "$src" "${SSH_USER:-rawwise}@$dst_host:$dst_path"
    fi
}

wait_for_port_remote() {
    local host="$1" port="$2" timeout="${3:-60}"
    echo -n "  Waiting for $host:$port ..."
    for ((i=0; i<timeout; i++)); do
        if ssh_cmd "$host" "nc -z localhost $port" 2>/dev/null; then
            echo " OK (${i}s)"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo " TIMEOUT (${timeout}s)"
    return 1
}

# ====== cluster 部署 ======
start_cluster() {
    log "启动 Cluster 模式 (分布式多节点)..."

    ensure_classpath

    local jar_name
    jar_name=$(resolve_jar_name)
    local jar_path="$RAW_ROOT/lib/$jar_name"
    local cp_file="$RAW_ROOT/lib/classpath.txt"

    if [ ! -f "$jar_path" ] || [ ! -f "$cp_file" ]; then
        err "JAR 或 classpath 不存在。请先运行: bash bin/deploy.sh package"
        exit 1
    fi

    local remote_dir="${REMOTE_RAWWISE_DIR:-/opt/rawwise}"
    local remote_warehouse="${REMOTE_WAREHOUSE:-/var/lib/rawwise/warehouse}"
    local remote_logs="${REMOTE_LOG_DIR:-/var/log/rawwise}"
    local ssh_user="${SSH_USER:-rawwise}"

    # JVM 基础参数
    local jvm_opens="--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
    local jvm_gc="-XX:+UseZGC -XX:+ZGenerational"

    # 解析节点
    local meta_ip="" fe_ip="" be_server_ip=""
    local node_ips=()
    local node_roles=()
    local node_ports=()
    local node_extras=()

    for entry in "${NODES[@]}"; do
        IFS=':' read -r role ip port extra <<< "$entry"
        node_roles+=("$role")
        node_ips+=("$ip")
        node_ports+=("$port")
        node_extras+=("${extra:-}")
        case "$role" in
            metastore)      meta_ip="$ip" ;;
            frontend)       fe_ip="$ip" ;;
            backend-server) be_server_ip="$ip" ;;
        esac
    done

    if [ -z "$meta_ip" ] || [ -z "$fe_ip" ] || [ -z "$be_server_ip" ]; then
        err "集群配置缺少必要节点: metastore/frontend/backend-server"
        err "当前: meta=$meta_ip fe=$fe_ip be_server=$be_server_ip"
        exit 1
    fi

    log "集群拓扑:"
    log "  Metastore:     $meta_ip:$METASTORE_GRPC_PORT"
    log "  Backend Server: $be_server_ip:$BACKEND_SERVER_GRPC_PORT"
    for i in "${!node_ips[@]}"; do
        [ "${node_roles[$i]}" = "backend-node" ] && \
            log "  Backend Node:   ${node_ips[$i]}:${node_ports[$i]} (${node_extras[$i]})"
    done
    log "  Frontend:       $fe_ip:$FRONTEND_PORT"

    # 1. 分发 JAR + classpath 到所有节点
    log "分发 JAR + classpath 到所有节点..."
    for ip in "${node_ips[@]}"; do
        ssh_cmd "$ip" "mkdir -p $remote_dir/lib $remote_warehouse $remote_logs" 2>/dev/null || true
        scp_file "$jar_path" "$ip" "$remote_dir/lib/" 2>/dev/null || warn "scp JAR 到 $ip 失败"
        scp_file "$cp_file" "$ip" "$remote_dir/lib/" 2>/dev/null || warn "scp classpath 到 $ip 失败"
    done
    log "分发完成"

    # 2. 启动 Metastore
    log "启动 Metastore ($meta_ip)..."
    ssh_cmd "$meta_ip" "pkill -f 'io.github.rawwise.metastore.server.GrpcServerMain' 2>/dev/null; sleep 1; \
        nohup java $jvm_opens $jvm_gc -Xmx2g \
        -Drawwise.metastore.type=rocksdb \
        -Drawwise.metastore.base-path=$remote_warehouse/metastore \
        -Drawwise.metastore.grpc-port=$METASTORE_GRPC_PORT \
        -cp \"$remote_dir/lib/$jar_name:\$(cat $remote_dir/lib/classpath.txt)\" \
        io.github.rawwise.metastore.server.GrpcServerMain \
        > $remote_logs/metastore.log 2>&1 &" 2>/dev/null
    wait_for_port_remote "$meta_ip" "$METASTORE_GRPC_PORT" 30 || err "Metastore 启动失败"

    # 3. 启动 Backend Server
    log "启动 Backend Server ($be_server_ip)..."
    local ha_flags=""
    if [ "${HA_ENABLED:-false}" = "true" ]; then
        ha_flags="-Drawwise.backend.ha.enabled=true"
        [ -n "${HA_OWNER_ID:-}" ] && ha_flags="$ha_flags -Drawwise.backend.ha.owner-id=$HA_OWNER_ID"
        [ -n "${HA_ADVERTISE_ADDRESS:-}" ] && ha_flags="$ha_flags -Drawwise.backend.ha.advertise-address=$HA_ADVERTISE_ADDRESS"
        ha_flags="$ha_flags -Drawwise.backend.ha.lease-ttl-ms=${HA_LEASE_TTL_MS:-10000}"
        ha_flags="$ha_flags -Drawwise.backend.ha.renew-interval-ms=${HA_RENEW_INTERVAL_MS:-3000}"
    fi

    ssh_cmd "$be_server_ip" "pkill -f 'io.github.rawwise.backend.server.Main' 2>/dev/null; sleep 1; \
        nohup java $jvm_opens $jvm_gc \
        -Xmx${SPARK_DRIVER_MEMORY:-4g} -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY:-2g} \
        -Drawwise.warehouse.path=$remote_warehouse \
        -Drawwise.metastore.type=grpc \
        -Drawwise.metastore.grpc-host=$meta_ip \
        -Drawwise.metastore.grpc-port=$METASTORE_GRPC_PORT \
        -Drawwise.backend.spark.master='${SPARK_MASTER:-local[*]}' \
        -Drawwise.backend.spark.driver-memory='${SPARK_DRIVER_MEMORY:-4g}' \
        ${SPARK_DRIVER_HOST:+-Drawwise.backend.spark.driver-host=$SPARK_DRIVER_HOST} \
        -Drawwise.auth.enabled=${AUTH_ENABLED:-false} \
        -Drawwise.auth.explicitly-unsafe-disable=${AUTH_UNSAFE_DISABLE:-true} \
        -Drawwise.backend.tls.enabled=${TLS_ENABLED:-false} \
        -Drawwise.frontend.ir.enabled=true \
        $ha_flags \
        -cp \"$remote_dir/lib/$jar_name:\$(cat $remote_dir/lib/classpath.txt)\" \
        io.github.rawwise.backend.server.Main --port $BACKEND_SERVER_GRPC_PORT \
        > $remote_logs/backend-server.log 2>&1 &" 2>/dev/null
    wait_for_port_remote "$be_server_ip" "$BACKEND_SERVER_GRPC_PORT" 60 || err "Backend Server 启动失败"

    # 4. 启动 Backend Nodes
    for i in "${!node_ips[@]}"; do
        [ "${node_roles[$i]}" != "backend-node" ] && continue
        local n_ip="${node_ips[$i]}"
        local n_port="${node_ports[$i]}"
        local n_extra="${node_extras[$i]}"
        local shard_id=""
        if [[ "$n_extra" == *"shard-id="* ]]; then
            shard_id="${n_extra#*shard-id=}"
            shard_id="${shard_id%%,*}"
        fi
        log "启动 Backend Node $n_ip:$n_port (shard=$shard_id)..."
        ssh_cmd "$n_ip" "pkill -f 'io.github.rawwise.backend.server.Main.*--mode node' 2>/dev/null; sleep 1; \
            nohup java $jvm_opens $jvm_gc \
            -Xmx${EMBEDDED_HEAP:-4g} -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY:-2g} \
            -Drawwise.warehouse.path=$remote_warehouse \
            -Drawwise.metastore.type=grpc \
            -Drawwise.metastore.grpc-host=$meta_ip \
            -Drawwise.metastore.grpc-port=$METASTORE_GRPC_PORT \
            -Drawwise.storage.lmdb.initial-map-size-mb=${STORAGE_LMDB_INIT_MB:-256} \
            -Drawwise.storage.lmdb.max-map-size-mb=${STORAGE_LMDB_MAX_MB:-4096} \
            -Drawwise.auth.enabled=${AUTH_ENABLED:-false} \
            -Drawwise.auth.explicitly-unsafe-disable=${AUTH_UNSAFE_DISABLE:-true} \
            -Drawwise.backend.tls.enabled=${TLS_ENABLED:-false} \
            -Drawwise.backend.node.shard-id=${shard_id:-0} \
            -Drawwise.backend.node.server-host=$be_server_ip \
            -Drawwise.backend.node.server-port=$BACKEND_SERVER_GRPC_PORT \
            -Drawwise.backend.node.id=node-${shard_id:-0} \
            -Drawwise.frontend.ir.enabled=true \
            -cp \"$remote_dir/lib/$jar_name:\$(cat $remote_dir/lib/classpath.txt)\" \
            io.github.rawwise.backend.server.Main --mode node \
            > $remote_logs/backend-node-${shard_id:-0}.log 2>&1 &" 2>/dev/null
        wait_for_port_remote "$n_ip" "$n_port" 60 || warn "Backend Node $n_ip 启动超时 (可能仍在初始化)"
    done

    # 5. 启动 Frontend
    log "启动 Frontend ($fe_ip)..."
    ssh_cmd "$fe_ip" "pkill -f 'io.github.rawwise.frontend.server.Main' 2>/dev/null; sleep 1; \
        nohup java $jvm_opens $jvm_gc \
        -Xmx2g \
        -Drawwise.warehouse.path=$remote_warehouse \
        -Drawwise.metastore.type=grpc \
        -Drawwise.metastore.grpc-host=$meta_ip \
        -Drawwise.metastore.grpc-port=$METASTORE_GRPC_PORT \
        -Drawwise.frontend.grpc.backend-host=$be_server_ip \
        -Drawwise.frontend.grpc.backend-port=$BACKEND_SERVER_GRPC_PORT \
        -Drawwise.auth.enabled=${AUTH_ENABLED:-false} \
        -Drawwise.auth.explicitly-unsafe-disable=${AUTH_UNSAFE_DISABLE:-true} \
        -Drawwise.frontend.grpc.tls.enabled=${TLS_ENABLED:-false} \
        -Drawwise.frontend.ir.enabled=true \
        -Drawwise.context.enabled=true \
        -cp \"$remote_dir/lib/$jar_name:\$(cat $remote_dir/lib/classpath.txt)\" \
        io.github.rawwise.frontend.server.Main \
        --port $FRONTEND_PORT --backend-host $be_server_ip --backend-port $BACKEND_SERVER_GRPC_PORT \
        > $remote_logs/frontend.log 2>&1 &" 2>/dev/null
    wait_for_port_remote "$fe_ip" "$FRONTEND_PORT" 30 || err "Frontend 启动失败"

    log "集群启动完成!"
    log "  Frontend HTTP: http://$fe_ip:$FRONTEND_PORT"
    log "  健康检查:      curl http://$fe_ip:$FRONTEND_PORT/health"
    log "  日志:          $remote_logs/ (各节点)"
}

# ====== stop ======
do_stop() {
    load_config

    if [ "$DEPLOY_MODE" = "standalone" ]; then
        bash "$SCRIPT_DIR/rawwise-stop.sh" --profile=dogfood
        return
    fi

    log "停止集群服务..."
    for entry in "${NODES[@]}"; do
        IFS=':' read -r role ip port extra <<< "$entry"
        case "$role" in
            frontend)       ssh_cmd "$ip" "pkill -f 'io.github.rawwise.frontend.server.Main' 2>/dev/null || true" ;;
            backend-server) ssh_cmd "$ip" "pkill -f 'io.github.rawwise.backend.server.Main' 2>/dev/null || true" ;;
            backend-node)   ssh_cmd "$ip" "pkill -f 'io.github.rawwise.backend.server.Main.*--mode node' 2>/dev/null || true" ;;
            metastore)      ssh_cmd "$ip" "pkill -f 'io.github.rawwise.metastore.server.GrpcServerMain' 2>/dev/null || true" ;;
        esac
        log "  $role@$ip 已停止"
    done
    log "集群已停止"
}

# ====== status ======
do_status() {
    load_config

    if [ "$DEPLOY_MODE" = "standalone" ]; then
        bash "$SCRIPT_DIR/rawwise-status.sh" --profile=dogfood
        return
    fi

    log "集群状态:"
    for entry in "${NODES[@]}"; do
        IFS=':' read -r role ip port extra <<< "$entry"
        local label="$role@$ip:$port"
        if ssh_cmd "$ip" "nc -z localhost $port" 2>/dev/null; then
            echo -e "  ${GREEN}●${NC} $label  ${GREEN}RUNNING${NC}"
        else
            echo -e "  ${RED}○${NC} $label  ${RED}DOWN${NC}"
        fi
    done

    local fe_ip=""
    for entry in "${NODES[@]}"; do
        IFS=':' read -r role ip port extra <<< "$entry"
        [ "$role" = "frontend" ] && fe_ip="$ip"
    done
    if [ -n "$fe_ip" ]; then
        log "健康检查: curl http://$fe_ip:$FRONTEND_PORT/health"
    fi
}

# ====== clean ======
do_clean() {
    load_config

    if [ "$DEPLOY_MODE" = "standalone" ]; then
        bash "$SCRIPT_DIR/rawwise-clean.sh" --profile=dogfood
        return
    fi

    local remote_warehouse="${REMOTE_WAREHOUSE:-/var/lib/rawwise/warehouse}"
    read -rp "确认清理所有节点数据 ($remote_warehouse)? [y/N] " confirm
    [ "$confirm" != "y" ] && [ "$confirm" != "Y" ] && { log "取消"; return; }

    for entry in "${NODES[@]}"; do
        IFS=':' read -r role ip port extra <<< "$entry"
        ssh_cmd "$ip" "rm -rf $remote_warehouse/* 2>/dev/null; find $remote_warehouse -name 'lock.mdb' -delete 2>/dev/null" 2>/dev/null || true
        log "  $role@$ip 数据已清理"
    done
}

# ====== init ======
do_init() {
    if [ -f "$CONF_FILE" ]; then
        warn "配置文件已存在: $CONF_FILE"
        read -rp "覆盖? [y/N] " confirm
        [ "$confirm" != "y" ] && [ "$confirm" != "Y" ] && { log "取消"; return; }
    fi
    mkdir -p "$(dirname "$CONF_FILE")"
    cp "$RAW_ROOT/deploy/deploy.conf.example" "$CONF_FILE"
    log "配置文件已创建: $CONF_FILE"
    log "请编辑此文件配置你的部署参数，然后运行: bash bin/deploy.sh start"
}

# ====== 主入口 ======
ACTION="${1:-start}"
case "$ACTION" in
    init)    do_init ;;
    package) load_config; do_package ;;
    start)   load_config
             [ "$DEPLOY_MODE" = "standalone" ] && start_standalone || start_cluster ;;
    stop)    do_stop ;;
    status)  do_status ;;
    restart) do_stop; sleep 2; load_config; [ "$DEPLOY_MODE" = "standalone" ] && start_standalone || start_cluster ;;
    clean)   do_clean ;;
    *)
        echo "用法: bash bin/deploy.sh {init|package|start|stop|status|restart|clean}"
        echo ""
        echo "  init     创建配置文件 deploy/deploy.conf"
        echo "  package  编译打包 JAR + classpath"
        echo "  start    启动服务 (根据 deploy.conf 中 DEPLOY_MODE 选择 standalone 或 cluster)"
        echo "  stop     停止服务"
        echo "  status   查看服务状态"
        echo "  restart  重启服务"
        echo "  clean    清理数据"
        exit 1
        ;;
esac

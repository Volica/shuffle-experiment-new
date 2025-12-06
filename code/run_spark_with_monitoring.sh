#!/bin/bash
# run_spark_with_monitoring.sh — 支持每台机器不同用户名

set -e

# === 日志配置 ===
EXPERIMENT_ID="exp_$(date +%Y%m%d_%H%M%S)"
RUN_LOG="./run_spark_with_monitoring_${EXPERIMENT_ID}.log"

# 仅将 stderr 追加到日志文件（保留 stdout 清晰）
exec 2> >(tee -a "$RUN_LOG")

# === 日志函数 ===
log_info()  { echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
log_warn()  { echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
log_error() { echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }
log_debug() { echo "[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# === 配置 ===
NODES=(
    "syzhu@49.52.27.113"
    "volica@49.52.27.60"
    "djk@49.52.27.65"
)

MONITOR_DIR="/tmp/monitor"
LOG_DIR="$MONITOR_DIR/log"
SCRIPT_PATH="$MONITOR_DIR/monitoring.sh"
TEST_SCRIPT="/home/syzhu/shuffle-experiment-new/test_basic copy.sh"

# 获取本机 IP（更健壮的方式）
LOCAL_IP=$(ip route get 8.8.8.8 2>/dev/null | awk '{print $7; exit}' || \
           ip addr show | grep 'inet ' | grep -v '127.0.0.1' | head -1 | awk '{print $2}' | cut -d'/' -f1 || \
           hostname -I | awk '{print $1}')
LOCAL_HOSTNAME=$(hostname -s)
log_debug "本机 IP: $LOCAL_IP, 主机名: $LOCAL_HOSTNAME"

# === 判断是否本地节点 ===
is_local_node() {
    local full_target="$1"
    local host_part="${full_target#*@}"

    if [[ "$host_part" == "$LOCAL_IP" ]] || \
       [[ "$host_part" == "$LOCAL_HOSTNAME" ]] || \
       [[ "$host_part" == "127.0.0.1" ]] || \
       [[ "$host_part" == "localhost" ]]; then
        return 0
    else
        return 1
    fi
}

# === 步骤 1：启动监控 ===
log_info "?? 在所有节点启动监控 ($EXPERIMENT_ID)"
declare -A MONITOR_PIDS

for target in "${NODES[@]}"; do
    host_part="${target#*@}"
    log_info "处理节点: $target (host=$host_part)"

    if is_local_node "$target"; then
        log_info "→ 识别为本地节点"
        mkdir -p "$LOG_DIR"
        nohup "$SCRIPT_PATH" -d "$LOG_DIR" -i 5 "$EXPERIMENT_ID" > /tmp/monitor_local.log 2>&1 &
        MONITOR_PIDS["$target"]=$!
        log_info "  本地监控 PID: ${MONITOR_PIDS[$target]}"
    else
        log_info "→ 远程节点，SSH 到 $target"

        # 测试连通性（带超时和非交互模式）
        if ! timeout 8 ssh \
            -o ConnectTimeout=5 \
            -o StrictHostKeyChecking=no \
            -o UserKnownHostsFile=/dev/null \
            -o GSSAPIAuthentication=no \
            -o BatchMode=yes \
            -T \
            "$target" "env -i /bin/sh -c 'true'" > /dev/null 2>&1; then
            log_error "SSH 连接失败或超时（>8秒）: $target"
            exit 1
        fi
         printf -v esc_LOG_DIR '%q' "$LOG_DIR"
    printf -v esc_SCRIPT_PATH '%q' "$SCRIPT_PATH"
    printf -v esc_EXPERIMENT_ID '%q' "$EXPERIMENT_ID"

    # 启动监控（使用 sh -c exec 替代 nohup）
    remote_cmd="mkdir -p $esc_LOG_DIR && sh -c 'exec $esc_SCRIPT_PATH -d $esc_LOG_DIR -i 5 $esc_EXPERIMENT_ID' >/tmp/monitor_remote.log 2>&1 &"
    ssh "$target" "env -i /bin/sh -c '$remote_cmd'"
    log_info "  已启动远程监控 on $target"
    fi
done

# === 步骤 2：运行实验 ===
log_info "?? 运行实验（模拟）"
log_info "?? 开始运行实验脚本: $TEST_SCRIPT"

# 检查脚本是否存在
if [[ ! -f "$TEST_SCRIPT" ]]; then
    log_error "? 实验脚本未找到: $TEST_SCRIPT"
    exit 1
fi

# 给监控一点启动时间（可选）
sleep 15

# 执行实验（捕获退出码）
if "$TEST_SCRIPT"; then
    log_info "? 实验成功完成"
else
    log_error "? 实验失败（退出码非0）"
    # 注意：这里可以选择是否继续收集日志
    # 如果希望无论成功失败都收集日志，不要 exit
fi
log_info "? 实验完成"

# === 步骤 3：停止监控 ===
log_info "?? 停止所有监控..."
for target in "${NODES[@]}"; do
    if is_local_node "$target"; then
        kill "${MONITOR_PIDS[$target]}" 2>/dev/null && log_info "  停止本地监控" || true
    else
        ssh "$target" "pkill -f 'monitoring.sh.*$EXPERIMENT_ID'" 2>/dev/null && \
            log_info "  停止远程监控 on $target" || \
            log_warn "  无远程监控进程 on $target"
    fi
done
sleep 2

# === 步骤 4：收集日志 ===
COLLECT_DIR="./collected_logs/$EXPERIMENT_ID"
mkdir -p "$COLLECT_DIR"
log_info "?? 收集日志到: $COLLECT_DIR"

for target in "${NODES[@]}"; do
    host_part="${target#*@}"
    REMOTE_CSV="$LOG_DIR/${EXPERIMENT_ID}_${host_part}.csv"
    LOCAL_CSV="$COLLECT_DIR/$(basename "$REMOTE_CSV")"

    if is_local_node "$target"; then
        if [[ -f "$REMOTE_CSV" ]]; then
            cp "$REMOTE_CSV" "$LOCAL_CSV"
            log_info "  ?? 本地日志: $LOCAL_CSV"
        else
            log_error "  ? 本地日志缺失: $REMOTE_CSV"
        fi
    else
        if ssh "$target" "[[ -f '$REMOTE_CSV' ]]"; then
            scp "$target:$REMOTE_CSV" "$LOCAL_CSV"
            log_info "  ?? 远程日志 from $target -> $LOCAL_CSV"
        else
            log_error "  ? 远程日志缺失 on $target: $REMOTE_CSV"
        fi
    fi
done

log_info "?? 全部完成！日志目录: $COLLECT_DIR"
ls -lh "$COLLECT_DIR"
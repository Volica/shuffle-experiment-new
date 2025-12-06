#!/bin/bash
# shuffle_file_test.sh - 测试Hash和Sort Shuffle的临时文件数量差异

set -e

JAR_PATH="target/scala-2.10/spark-shuffle-experiment-2.0.0.jar"
MASTER="spark://49.52.27.113:7077"
TEST_DIR="logs/shuffle_file_test_$(date '+%Y%m%d_%H%M%S')"
# TEST_DIR="logs/shuffle_file_test_20251204_044258"
mkdir -p $TEST_DIR

echo "=== Shuffle临时文件数量对比测试 ==="
echo "测试目录: $TEST_DIR"
echo "测试目标: 比较Hash和Sort Shuffle产生的临时文件数量"
echo ""

# 获取Shuffle临时文件数量
get_shuffle_file_count() {
    local log_file=$1
    local mode=$2
    local size=$3  # <--- 新增参数: 数据集大小
    
    # echo "正在分析 [${mode} - ${size}] 日志..."
    
    # 1. 提取耗时
    local duration=$(grep "执行耗时:" "$log_file" | awk -F': ' '{print $2}' | sed 's/ms//')
    
    # 2. 提取数据量
    local shuffle_bytes=$(grep "Shuffle数据量:" "$log_file" | awk -F': ' '{print $2}')
    
    # 3. 提取核心指标：峰值文件数
    local file_count=$(grep "峰值Shuffle文件数:" "$log_file" | awk -F': ' '{print $2}' | sed 's/ 个//')
    
    # 4. 提取峰值大小
    local file_size=$(grep "峰值Shuffle文件大小:" "$log_file" | awk -F': ' '{print $2}')
    
    # echo "  > 模式: $mode ($size)"
    # echo "  > 耗时: ${duration}ms"
    # echo "  > 文件数: ${file_count}"
    # echo "  > 总大小: ${file_size}"

    # 返回CSV格式数据: 模式,数据集,耗时,文件数,数据量
    echo "$mode,$size,${duration},${file_count},${shuffle_bytes}"
}

# 执行单次Shuffle测试
run_shuffle_test() {
    local mode=$1
    local size=$2
    local description="$3 ($size)"

    # 🔥 1. 每次运行前清理环境，防止上一轮的文件干扰统计
    echo ">>> [准备] 清理环境 (rm -rf /tmp/spark/work/*) ..."
    rm -rf /tmp/spark/work/*
    sleep 2
    
    echo "--- 测试开始: ${description} ---"
    
    # 🔥 2. 修改日志文件名，包含 mode 和 size
    local log_file="${TEST_DIR}/shuffle_${mode}_${size}.log"
    local start_time=$(date +%s)
    
    echo "日志路径: $log_file"
    echo "执行参数: mode=${mode}, size=${size}"
    
    # 执行Spark作业
    spark-submit \
        --class edu.ecnu.MainEntry \
        --master $MASTER \
        --deploy-mode client \
        --executor-memory 1G \
        --driver-memory 512M \
        --executor-cores 2 \
        --conf spark.executor.instances=2 \
        --conf spark.dynamicAllocation.enabled=false \
        --total-executor-cores 4 \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.manager=${mode} \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir="${TEST_DIR}/events_${mode}_${size}" \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.shuffle.consolidateFiles=false \
        --conf spark.shuffle.sort.bypassMergeThreshold=1 \
        $JAR_PATH "file" "$mode" "$size" 2>&1 | tee "$log_file"
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "作业执行时间: ${duration}秒"
    echo "测试结束: ${description}"
    echo ""
    
    # 等待资源释放
    sleep 5
}

# === 循环测试 ===

# medium large

echo "=== 测试阶段 1: Hash Shuffle ==="
for size in small-x small; do
    run_shuffle_test "hash" "$size" "Hash Shuffle"
done

echo "=== 测试阶段 2: Sort Shuffle ==="
for size in small-x small; do
    run_shuffle_test "sort" "$size" "Sort Shuffle"
done

# === 结果分析 ===
echo "=== 测试结果分析 ==="

# 创建结果汇总文件
result_file="${TEST_DIR}/shuffle_comparison.csv"
# 🔥 3. CSV Header 增加 "数据集" 列
echo "模式,数据集,耗时(ms),峰值文件数,Shuffle数据量" > "$result_file"

# 🔥 4. 双层循环遍历所有 6 个日志文件
for mode in hash sort; do
    for size in small medium large; do
        log_file="${TEST_DIR}/shuffle_${mode}_${size}.log"
        if [ -f "$log_file" ]; then
            # 传入 size 参数
            get_shuffle_file_count "$log_file" "$mode" "$size" >> "$result_file"
        else
            echo "警告: 日志文件 $log_file 不存在 (可能该任务运行失败)"
        fi
    done
done

# # 生成比较报告
# report_file="${TEST_DIR}/shuffle_file_analysis.md"
# cat > "$report_file" << EOF
# # Shuffle临时文件数量测试报告

# ## 测试概述
# - 测试时间: $(date)
# - 测试目标: 比较不同Shuffle管理器产生的临时文件数量
# - 测试目录: ${TEST_DIR}

# ## 性能数据汇总
# \`\`\`csv
# $(cat "$result_file")
# \`\`\`

# ## 结果分析

# ### 1. Hash Shuffle (Spark 1.x Legacy)
# - **机制**: 为每个 Reduce 分区创建一个独立文件。
# - **文件数公式**: Map任务数 × Reduce分区数
# - **本次实验观察**: 应该能看到文件数量随 Map 分区数增加而剧烈增加（达到数千个）。
# - **缺点**: 产生海量随机小IO，容易耗尽 Inode，内存消耗大（Buffer多）。

# ### 2. Sort Shuffle (Standard)
# - **机制**: 每个 Map 任务只产生 1 个数据文件和 1 个索引文件。
# - **文件数公式**: 2 × Map任务数
# - **本次实验观察**: 无论 Reduce 分区多少，文件数都很少（通常几十个）。
# - **优点**: 顺序IO，文件管理高效，适合大规模集群。

# EOF

# echo "=== 全部测试完成 ==="
# echo "汇总CSV: $result_file"
# echo "分析报告: $report_file"
# echo "日志目录: $TEST_DIR"

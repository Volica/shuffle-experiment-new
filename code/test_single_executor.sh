#!/bin/bash
# test_single_executor.sh - 测试单个Executor的不同核心数

set -e

JAR_PATH="target/scala-2.10/spark-shuffle-experiment-1.0.0.jar"
MASTER="spark://49.52.27.113:7077"
TEST_DIR="test_single_executor_$(date '+%Y%m%d_%H%M%S')"
mkdir -p $TEST_DIR

echo "=== 单个Executor不同核心数测试 ==="
echo "测试目录: $TEST_DIR"
echo ""

test_single_executor() {
    local cores=$1
    local memory=$2
    local shuffle_type=$3
    
    local name="single_executor_${cores}cores_${memory}_${shuffle_type}"
    local log_file="${TEST_DIR}/${name}.log"
    
    if [ "$shuffle_type" = "hash" ]; then
        local shuffle_conf="--conf spark.shuffle.manager=hash --conf spark.shuffle.consolidateFiles=true"
    else
        local shuffle_conf="--conf spark.shuffle.manager=sort --conf spark.shuffle.sort.bypassMergeThreshold=200 --conf spark.shuffle.compress=true"
    fi
    
    echo "测试: 1个Executor, ${cores}核心, ${memory}内存, ${shuffle_type} shuffle"
    
    spark-submit \
        --class edu.ecnu.ShuffleExperiment \
        --master $MASTER \
        --deploy-mode client \
        --executor-memory $memory \
        --driver-memory 512M \
        --executor-cores $cores \
        --conf spark.executor.instances=1 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        $shuffle_conf \
        $JAR_PATH "$name" > "$log_file" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "? 成功"
        if grep -q "结果" "$log_file"; then
            grep "结果" "$log_file" | tail -1
        fi
    else
        echo "? 失败"
        echo "错误:"
        tail -3 "$log_file"
    fi
    echo ""
    
    sleep 10
}

# 测试不同核心配置（从少到多）
echo "===== 测试Hash Shuffle ====="
test_single_executor 1 "1G" "hash"
test_single_executor 2 "1G" "hash"
test_single_executor 3 "1G" "hash"
#test_single_executor 4 "1G" "hash"
  # 更多核心需要更多内存

echo "===== 测试Sort Shuffle ====="
test_single_executor 1 "1G" "sort"
test_single_executor 2 "1G" "sort"
test_single_executor 3 "1G" "sort"
#test_single_executor 4 "1G" "sort"


echo "=== 测试完成 ==="
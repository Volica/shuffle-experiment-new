# 题目

Spark Shuffle 算法比较

# 研究目的

本研究的核心目的是深入比较 Apache Spark 中两种核心的 Shuffle 实现机制：基于 Hash 的 Shuffle 和 基于 Sort 的 Shuffle 。  
虽然 Spark 社区在 1.2.0 版本后将默认机制切换为 Sort Shuffle，但在非排序需求或者小规模数据的场景下，Hash Shuffle 仍具有理论上的低延迟优势。  
本研究旨在通过可控的实验，量化比较两者在**不同数据规模、不同内存限制以及数据倾斜**场景下的性能表现（如执行时间、文件数量、内存稳定性），并且尝试探讨它们各自的优缺点及适用场景。
# 实验设计

选取合适的计算负载，在不同规模的数据集下分别采用基于Hash和基于Sort的Shuffle 算法。通过记录Shuffle数据量、内存使用量及作业执行时间等指标，对比分析两种Shuffle算法的性能特征与适用场景。

# 研究内容

1、两种 Shuffle 算法的机制对比：深入分析 Hash Shuffle 的缓冲区管理与文件生成机制。分析 Sort Shuffle 的内存排序、溢写（Spill）及归并机制。

2、基础性能基准测试：固定配置下，不同数据规模两种shuffle的执行时间，Shuffle数据量，临时文件个数，吞吐量。

3、固定配置下，数据规模较大时两种shuffle的CPU空闲率与I/O等待数量以及负载监控情况体现其性能差异。

4、内存限制的情况下，两种shuffle的表现。

5、数据倾斜测试：在不同程度的数据倾斜下，两种 Shuffle 机制的处理效率及任务成功率。


# 项目结构：

## 设计模式

**第一段：架构核心与入口设计**  
以**分派器模式**为核心，构建了一个统一、灵活的架构。中央控制器`MainEntry`作为唯一入口，负责接收请求、解析参数，并动态路由到不同的具体任务。这种设计不仅遵循了单一职责原则，确保了模块间的清晰界限，还为系统提供了统一的参数校验和错误处理机制，极大地提升了整体的可维护性和可扩展性。

**第二段：策略化的任务实现**  
在具体任务执行层面，广泛运用了**策略模式**。例如，对于Shuffle过程的监控，系统定义了多种独立的监控策略，如基于文件数量的`FileCountMonitor`和基于内存压力的`MemoryStressMonitor`。这些策略可以像插件一样被灵活选择和切换，无需修改主流程代码。这种设计使得系统能够轻松适应不同的监控需求和实验场景，例如进行数据倾斜实验。

**第三段：松耦合的支撑机制**  
**观察者模式**以非侵入方式获取运行时状态，集成Spark的监听框架。自定义的监听器（如`FileCountMonitorMetricsListener`）能够实时、透明地采集Shuffle字节数等关键性能指标。同时，**工厂模式**被用于数据准备阶段，由`DataGenerator`工厂统一创建不同分布（均匀或倾斜）和规模的测试数据集。这两种模式共同构成了系统的支撑层，通过松耦合的方式提供了必要的监控数据和实验素材，保证了核心逻辑的纯净与高效。

## 项目说明

**数据**：DataGenerator.scala

每个实验对应一个脚本+一个文件

**基础**：
- 涉及文件：
    - BasicExperiment.scala   
    - test_basic.sh 
    - FileCountMonitor.scala  
    - test_file_monitor.sh
- 动机：做这个实验的原因是想获取一下shuffle在work目录产生的临时文件量，在使用最开始的UserBehavior数据集发现hash和sort的shuffle方式都会产生几十个临时文件，与预期不符合。
- 做法：
    - 改进了数据集的生成方式和分区数量。
    - 在三个热点key上分配95%的数据量，制造数据倾斜。目前观察到在medium数据集上，内存设置为4G仍然会发生OOM。
- 结果：提供在4个数据规模1w 10w 100w 500w下的两种shuffle类型的运行时间、shffule数据量、峰值文件数量、峰值文件大小。

**内存限制**：
- 涉及文件：
    - MemoryStress.scala  
    - test_moemory_stress.sh
需要探索不同内存分配对两种shuffle的影响的异同点。

**数据倾斜**：
- 涉及文件：
    - SkewExperiment.scala  
    - run_skew_experiment.sh
- 说明：引入 Zipf（齐夫）分布生成器，模拟不同程度的数据倾斜（Skew=0.0 均匀, 1.0 中度, 2.0 重度），以及不同规模的数据集大小（small,medium,large），进行对比实验。
# 实验

## 实验环境

硬件配置：
- 集群架构：3 节点集群（Spark Standalone 模式）。
- Master/Driver 节点: IP 49.52.27.113 (Hostname: ds113)。
- Worker 节点: IP 49.52.27.60（Hostname: volica） 和 49.52.27.65（Hostname: hygon7490）。

- spark worker配置
- export SPARK_WORKER_CORES=8
- export SPARK_WORKER_MEMORY=10g
- export SPARK_DAEMON_MEMORY=1g

CPU 核数：
实验中配置总共使用 8 核（--total-executor-cores 8），每个 Executor 分配 2 核，通常启动 4 个 Executor 实例。

内存大小：
- Executor 内存: 默认为 1GB（--executor-memory 1G），在压力测试中调整为 512M - 4G，在数据倾斜测试中调整为8G(为了测试large数据集而在该实验中统一扩大）。
- Driver 内存: 512MB。
- 存储：使用本地磁盘作为 Shuffle 临时目录（/tmp/spark/work）。

软件配置：
- 操作系统：Linux 
- JDK 版本：JDK 1.8
- Spark 版本：1.6.3。
- Scala 版本：2.10.5。
- 构建工具：sbt 0.13.18。


## 实验负载

本实验采用自定义的数据生成器和计算逻辑，旨在剥离计算本身的开销，专注于 Shuffle 阶段的性能。

数据集 (DataGenerator)：
- 数据类型：模拟用户行为日志。Schema 为 (key, value, category, payload)。
- Payload：为了增加 Shuffle Write 的数据量压力，每条记录包含一个约 1KB 的随机生成的字节数组字符串 (payload)。

数据分布：
- Uniform (均匀): Key 使用 UUID 生成，保证高基数且均匀分布，用于测试文件数量爆炸。
- Skew (倾斜): Key 使用 Zipf 算法采样（范围 0-100,000），通过调整参数 skew (0.0, 1.0, 2.0) 模拟热点 Key 现象。

数据规模：
- Small: 10万条记录 (约 100MB)。
- Medium: 100万条记录 (约 1GB)。
- Large: 500万条记录 或 5000万条记录 (不同批次实验中有所调整，Large 组 Shuffle 数据量约为 7.35 GB)。

工作负载 (Workload)：
- 核心算子：groupByKey(numPartitions)。选择该算子是因为它会强制进行全量 Shuffle（相比 reduceByKey 没有 Map 端预聚合），能够产生最大的网络传输和磁盘读写压力。
- 分区设置：显式指定分区数（如 Large 数据集设为 200 或 500），以确保 Hash Shuffle 产生足够多的 Bucket 文件 (M * R)。
- 预热机制：在正式计时前，先执行 df.cache() 和 df.count()，确保数据生成和 JVM 启动的开销不计入 Shuffle 性能测试结果。


## 实验步骤

### 进程信息
![替代文本](/picture/图片%201.png)
![替代文本](/picture/图片%202.png)
![替代文本](/picture/图片%203.png)
### 作业执行

![替代文本](/picture/图片%204.png)
### 环境配置

#### 下载jdk8

sudo apt install openjdk-8-jdk

sudo update-alternatives --config java

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

![替代文本](/picture/图片%205.png)
#### 下载 SBT 0.13.18

wget https://github.com/sbt/sbt/releases/download/v0.13.18/sbt-0.13.18.tgz

vim ~/.bashrc

export PATH=/tmp/sbt/bin:$PATH

source ~/.bashrc

![替代文本](/picture/图片%206.png)
#### 下载spark

wget https://archive.apache.org/dist/spark/spark-1.6.3/spark-1.6.3-bin-hadoop2.6.tgz

tar -xvzf spark-1.6.3-bin-hadoop2.6.tgz

sudo mv spark-1.6.3-bin-hadoop2.6 /tmp/spark

echo "export SPARK_HOME=/tmp/spark" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$PATH" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc

spark-submit --version
![替代文本](/picture/图片%207.png)

### 机制对比

实现Hash Shuffle 的缓冲区管理与文件生成机制；与Sort Shuffle 的内存排序、溢写（Spill）及归并机制。

### 基准测试

在不同数据规模（Small, Medium, Large）下，对比两者的执行耗时与吞吐量。
![替代文本](/picture/图片%208.png)
![替代文本](/picture/图片%209.png)

### CPU空闲率与I/O等待数量以及负载监控情况

在本机以及远程主机上部署监控节点随着测试开始与结束运行，最后传回结果，基于mpstat进行监控。
![替代文本](/picture/图片%2010.png)

### 内存限制

对实验中两种 Shuffle 的整体趋势、内存敏感性差异、Sort 的 spill 与 Hash 无 spill 的原因，以及两者在低内存下 OOM 的边界进行了系统性的总结与解释。

![替代文本](/picture/图片%2011.png)

### 数据倾斜：

- 选取small,medium,large数据集时，hash和sort两种模式，skew=0/1/2的情况下，执行脚本./run_skew_experiment.sh。
- 在这18组对比中，仅large数据集下hash和sort shuffle无法成功完成任务，其他均能正常完成。
- 为了正常运行large数据集做的尝试：增大运行允许时间，与executor_memory到8G均不成功。
运行截图如下：

![替代文本](/picture/图片%2012.png)

## 实验结果与分析

### 基准测试

**实验参数**：

| 数据量               | 500w |
| ----------------- | ---- |
| Executor内存        | 8G   |
| Executor核心        | 4    |
| Executor数量        | 2    |
| numPartitions     | 200  |
| shufflePartitions | 500  |

**实验结果**

![替代文本](/picture/图片%2013.png)
![替代文本](/picture/图片%2014.png)

**实验结果分析**

**Hash Shuffle 分析**

实验数据表明，Hash Shuffle 在本测试场景下表现出显著的性能瓶颈。核心制约因素在于中间文件的生成机制。实验记录显示，随着作业推进，Shuffle 文件数量呈现爆发式增长，最终峰值达到 51,592 个。这一现象符合 Hash Shuffle 的理论模型，即每个 Map Task 需为每个 Reduce Partition 创建独立的输出文件（文件总数 N=M×R ），由此产生的海量小文件导致了严重的磁盘碎片化。  

从操作系统层面分析，数以万计的文件句柄同时打开与关闭引发了密集的系统调用，导致巨大的内核态上下文切换开销。更关键的是，这种文件分布模式将磁盘 I/O 操作强制转变为低效的随机写，磁盘磁头频繁寻道显著降低了吞吐量，从而抵消了 Hash Shuffle 因省略排序步骤所带来的 CPU 收益。

**Sort Shuffle 分析**

相比之下，Sort Shuffle 展现了更优的执行效率与系统稳定性。数据监测显示，其 Shuffle 文件数量稳定维持在 800 左右，未随数据量增加而波动。这归因于 Sort Shuffle 在 Map 端引入的排序与Merge机制：无论下游分区数量如何，每个 Map Task 最终仅输出一个整合后的数据文件及一个索引文件。这种机制在 I/O 层面实现了质的优化，将写入操作由多文件随机写转变为单文件顺序写，极大提升了磁盘 I/O 带宽利用率。 

此外，在内存管理方面，尽管 Sort Shuffle 引入了排序计算的内存开销，但其 GC 累积时间远低于 Hash Shuffle。这表明，Hash Shuffle 为维护成千上万个并发写入缓冲区所消耗的内存对象开销，远大于 Sort Shuffle 排序缓冲区的内存压力。


### 分区基数实验

**1. 资源竞争与内存溢写阶段 (Under-partitioning Regime, N≤20)** 

在低分区数区间，两种 Shuffle 机制均受限于单任务数据负载过高（Per-task Data Volume）。实验数据显示，当分区数 N=10 时，Executor 内存不足以容纳 Shuffle Map 输出，导致了约 1.2 GB 的磁盘溢写（Disk Spill）。此时，系统的主要瓶颈为 **I/O 延迟**与**频繁的 Full GC**（GC 耗时均达 20s以上）。在此阶段，算法差异被硬件资源瓶颈掩盖，两者性能均表现较差。

**2. 最佳性能区间与算法优势 (Optimal Range, 40≤N≤160)** 

随着分区数增加，内存压力缓解，两种机制的性能差异显现。Hash Shuffle在此区间展现出显著的**吞吐量优势**，在 N=80 时达到实验最低延迟（12.1s），相较于 Sort Shuffle 快约 40%。这一优势归因于 Hash Shuffle **免排序（Sort-free）** 的设计特性，省去了计算密集型的排序与归并操作，实现了内存到缓冲区的直接映射。

**3. 扩展性瓶颈与系统开销 (Over-partitioning & Scalability, N≥640)** 

在高分区数区间，实验结果揭示了 Hash Shuffle 严重的**扩展性缺陷（Scalability Collapse）**。当 N=1000 时，Hash Shuffle 的延迟激增至 121s（增长 10 倍），且伴随异常高的 GC 耗时（240s）和反序列化开销。这验证了 **“缓冲区爆炸（Buffer Explosion）”** 现象：Hash Shuffle 需维护 M×R 个写缓冲区，导致堆内存碎片化严重及对象创建开销剧增。

相比之下，Sort Shuffle 展现了极强的**鲁棒性**。
其延迟在 N=1000 时仍稳定在 26s，未出现显著退化。这得益于其固定的内存管理模型（每任务仅维护单一缓冲区及溢写文件），成功将内存消耗与分区数量解耦。

**结论** 

实验表明，Hash Shuffle 的性能曲线呈典型的 **“U型”**，在特定区间具备性能优势，但缺乏高并发场景下的扩展性；Sort Shuffle 则呈 **“L型”** 平滑曲线，虽然引入了排序开销，但以牺牲少量峰值性能为代价，换取了在大规模分区场景下的线性扩展能力与稳定性。

![替代文本](/picture/图片%2015.png)
![替代文本](/picture/图片%2016.png)


### CPU空闲率与I/O等待数量以及负载监控情况

![替代文本](/picture/图片%2017.png)
![替代文本](/picture/图片%2018.png)
**对比**

在对比分析 Sort Shuffle 与 Hash Shuffle 的 CPU 利用率表现时，可以发现两者在 IO 等待和 CPU 空闲率方面存在明显差异。就 IO 等待而言，Sort Shuffle 整体较低且表现平稳，说明其 IO 操作效率更高；而 Hash Shuffle 的 IO 等待明显高于 Sort，且存在显著的峰值，尤其在部分时段 IO 等待极高，反映出其磁盘操作压力较大。

**CPU空闲率分析**

在 CPU 空闲率方面，Sort Shuffle 的空闲率维持在较高水平，约为 30% 左右；相比之下，Hash Shuffle 的 CPU 空闲率显著偏低，平均值约在 25%，其 CPU 使用率明显更高，空闲率相比 Sort 大约低 20% 至 30%。整体来看，Sort Shuffle 在性能上表现出以下优势：其 IO 效率更高，等待时间更短，磁盘操作更为高效；CPU 利用率更为合理，不会过度占用 CPU，系统资源分配相对均衡；整体运行稳定性更好，IO 与 CPU 的使用曲线较为平稳，未出现剧烈波动。而 Hash Shuffle 的劣势则主要体现在：IO 瓶颈较为突出，高 IO 等待表明磁盘容易成为性能瓶颈；CPU 处于过度使用状态，低空闲率意味着 CPU 大量时间用于处理 shuffle 任务；同时其性能波动较大，存在明显的高峰，稳定性相对较差。

**差异分析**

这些差异背后的主要原因在于，Sort Shuffle 通过文件合并机制有效减少了小文件数量，从而降低了 IO 开销；而 Hash Shuffle 需要为每个 reduce 任务创建独立文件，导致产生大量小文件，增加了 IO 负担。在 CPU 方面，Hash Shuffle 由于需维护大量文件句柄和缓冲区，占用了较多资源；Sort Shuffle 则借助排序与合并操作，减少了内存与 CPU 的额外开销。

**总结**

综合以上分析，建议在内存条件允许的情况下优先选用 Sort Shuffle；Hash Shuffle 仅建议在 reduce 任务数量非常有限时酌情考虑使用。
![替代文本](/picture/图片%2019.png)

**总体数据分析**

在负载水平方面，Sort Shuffle 与 Hash Shuffle 的表现存在显著差异。总体来看，Sort Shuffle 的负载整体较低，大部分时间维持在 0 到 2 的区间内；而 Hash Shuffle 的负载水平显著更高，其峰值甚至达到 16，是 Sort Shuffle 的 8 倍以上。从平均值来看，两者负载平均值近似。

**稳定性分析**

在负载稳定性上，Sort Shuffle 表现非常稳定，波动幅度很小，仅在少数时间点有轻微上升，整体呈平稳的直线状。相比之下，Hash Shuffle 的负载波动剧烈，呈现出明显的锯齿状特征，频繁出现高峰值，并且具有清晰的周期性波动模式。就峰值对比而言，Sort Shuffle 的最大峰值大约在 4 到 6 之间，而 Hash Shuffle 的最大峰值达到 16，其最高负载是 Sort Shuffle 的 4 倍。

**资源使用模式分析**

从资源占用模式分析，Sort Shuffle 的资源使用效率较高，负载平缓，没有明显的资源竞争现象；而 Hash Shuffle 则存在明显的资源竞争和拥塞，具体表现为周期性的负载高峰。综合性能表现可以得出结论，Sort Shuffle 具有极低的系统负载，对集群资源影响小；它展现出出色的稳定性，无剧烈波动，适合在生产环境中使用；同时其负载可预测，便于进行系统容量规划。反观 Hash Shuffle，其劣势在于负载过高，显著增加了系统压力；负载不稳定且波动大，导致性能难以预测；同时存在严重的资源竞争，周期性高峰表明系统遇到了资源瓶颈。

**差异原因分析**

这种负载差异主要源于以下几方面的技术原因。在文件管理方式上，Hash Shuffle 需要为每个 reduce 任务创建单独的文件，导致产生大量小文件，进而带来高昂的元数据管理开销；而 Sort Shuffle 通过合并文件，最终生成少量大文件，显著降低了元数据开销。在内存使用方面，Hash Shuffle 需要大量内存来存储文件句柄和缓冲区，而 Sort Shuffle 则通过排序和合并操作优化了内存使用效率。此外，在网络与磁盘 IO 模式上，Hash Shuffle 的随机 IO 模式容易引发资源竞争，而 Sort Shuffle 的顺序 IO 模式则更为高效。

### 内存限制

![替代文本](/picture/图片%2020.png)

从**总体执行效率**来看，Sort Shuffle 表现出对 Hash Shuffle 的显著优势且性能更稳定。根据数据，在内存充足的理想条件下（8GB），Sort Shuffle 的执行时间为 67,846ms，比 Hash Shuffle 的 75,127ms 快约 $10.7\%$。这种差异的关键在于它们采用的 I/O 模型不同。Hash Shuffle 必须为每一个目标 Reduce 任务维护独立的缓冲区，导致产生了数量庞大（超过 50,000 个）但体积微小（平均 76KB）的输出文件，由此引发了频繁且低效的随机磁盘 I/O 开销。相比之下，Sort Shuffle 则通过对数据进行排序和归并，成功地将输出文件数量控制在极少的水平（约 400 个），且文件平均大小大幅增加至 10MB 左右，极大地优化了磁盘的顺序读写效率，因此在整体任务吞吐量上更胜一筹。

**Hash Shuffle 受内存限制影响极大，而 Sort Shuffle 对内存的耐受性更好。**
Hash Shuffle 对内存资源的依赖性极高，其性能在低内存环境下会遭受灾难性的冲击。当内存从 8GB 降至 512MB 时，Hash Shuffle 的执行时间暴增了 $126\%$，其根源在于 缺乏主动的磁盘溢写机制。由于 Hash Shuffle 必须在内存中维持所有的输出缓冲区，在内存吃紧时，唯一能做的就是等待 JVM 频繁地执行高开销的 Full Garbage Collection (GC) 来回收空间。数据显示，在 512MB 内存下，其 GC 时间飙升至 4 秒，是高内存环境下的近百倍，正是这种长时间的 GC 暂停导致了程序运行的严重阻塞。而 Sort Shuffle 在相同的低内存环境下，通过主动将 7GB 多的数据溢写到磁盘，成功地将 GC 时间控制在 0.3 秒的较低水平，以相对较小的 I/O 成本避免了高频 GC 的性能惩罚。

两种 Shuffle 在 256MB 内存下都会 OOM，说明该任务的数据规模存在一个不可跨越的最小内存边界。尽管 Sort Shuffle 具备出色的内存耐受性，但两种 Shuffle 模式在 256MB 内存下均无法避免 OOM（内存溢出）错误，无论采用何种优化策略，任何大数据任务都需要一定的基础内存来维持 JVM 的运行、维护数据结构、以及执行基础的排序和缓冲区管理操作。

### 数据倾斜

选取small,medium,large数据集时，hash和sort两种模式，skew=0/1/2的情况下，18组实验数据csv文件的内容如下图所示：
![替代文本](/picture/图片%2021.png)
下图展示 Apache Spark Web UI 中的 Event Timeline，展示了由于数据倾斜所带来的长尾任务现象——这表明绝大多数任务在几毫秒或几秒内就完成了（左侧），但有少数几个任务（右侧）运行了非常久。整个 Stage 的完成时间取决于最慢的那个任务。
![替代文本](/picture/图片%2022.png)
python绘图的结果如下所示：
![替代文本](/picture/图片%2023.png)
#### 小数据集性能分析

见上图。

- **现象**：在小数据集下，Hash Shuffle (2.8s - 3.1s) 普遍略快于 Sort Shuffle (3.0s - 3.3s)。
- **合理性分析**：
- **低开销优势**：小数据集（约 150MB）完全可以放入内存。Hash Shuffle 不需要进行排序（Sort），直接根据 Hash 值将数据写入缓冲区，省去了排序的 CPU 开销。
- **文件数未达瓶颈**：此时 Map 任务数较少，Hash Shuffle 产生的文件数量（$M \times R$）还在操作系统可接受范围内，随机写 I/O 的劣势不明显。
- **结论**：**对于小规模数据作业，Hash Shuffle 确实具有低延迟的优势**，验证了其存在的价值。

![替代文本](/picture/图片%2024.png)
#### 中数据集性能分析

见上图。

- **现象**：在 Medium 数据集下，两者性能互有胜负，且呈现出有趣的波动。
    - Skew=0.0（均匀）：两者几乎持平（\~14.4s）。
    - Skew=1.0&2.0（倾斜）：Hash Shuffle (10.6s) 反而比 Sort Shuffle (11.3s) 更快，且都比均匀分布快。
- **合理性分析**：
    - 这可能是因为 Zipf 分布导致数据集中在少数几个 Key 上，这些 Key 被分配到了同一个 Reduce 分区。对于 Hash Shuffle 来说，这意味着大量的写操作集中在少数几个输出文件上，变相实现了“顺序写”，减少了磁盘寻道时间。
    - 而 Sort Shuffle 依然需要付出排序的固定成本。
- **结论**：在内存充足且数据量适中（\~1.5GB）时，Hash Shuffle 依然表现强劲。

![替代文本](/picture/图片%2025.png)
#### 大数据集性能分析

见上图。
- 在 Skew=0.0 时，Hash (43.0s) 与 Sort (43.5s) 性能接近。
- 但随着数据量增大（7.36GB），Sort Shuffle 的文件合并机制（每个 Map 仅输出 1 个文件）在 I/O 效率上的长远优势开始显现，而 Hash Shuffle 产生的文件数量剧增。


#### 大数据集稳定性分析

large数据集下不同倾斜程度的任务成败见下图。
![替代文本](/picture/图片%2026.png)
**(hash,large,skew=2)时的问题如下图所示**：
![替代文本](/picture/图片%2027.png)

**(sort,large,skew=2)时的问题如下图所示**：
![替代文本](/picture/图片%2028.png)
![替代文本](/picture/图片%2029.png)

**日志分析 (Hash Shuffle)**：

- 报错：`java.lang.OutOfMemoryError: GC overhead limit exceeded`。
- 原因：Hash Shuffle 在 Reduce 端（`groupByKey`）试图将单一倾斜 Key 的所有 Values 全部拉入内存。由于缺乏溢写（Spill）机制，内存瞬间被撑爆。

**日志分析 (Sort Shuffle)**：
- 报错：`org.apache.spark.shuffle.FetchFailedException: java.io.FileNotFoundException` 以及 `ExecutorLost`。
- 原因：Sort Shuffle 虽然有 Spill 机制，但在处理极度倾斜的 Key 时，产生的数据块（Block）依然过大，导致：
    - **磁盘 I/O 拥塞**：单个 Executor 处理的数据量远超其他节点。
    - **网络超时**：Reduce 端拉取这个巨大的 Block 时，容易导致连接超时或重置（`Connection reset by peer`）。
    - **节点失联**：过高的 GC 压力导致 Executor 无法及时发送心跳，被 Driver 判定为死亡。

![替代文本](/picture/图片%2030.png)

## 结论

### 基准测试

本实验证实，在达到一定并行度规模后，文件系统交互开销成为 Shuffle 性能的首要瓶颈。Hash Shuffle 的不可扩展性在其 M×R 的文件生成模式中得到验证：随着并行度的提升，指数级增长的文件数量会导致 I/O 延迟急剧恶化。相反，Sort Shuffle 虽然引入了额外的排序计算成本，但通过文件合并策略换取了高效的顺序 I/O 和更低的文件系统负载，证明了其在大规模分布式计算场景下的鲁棒性。


### CPU空闲率与I/O等待数量以及负载监控情况

从系统资源使用情况来看，Sort Shuffle 与 Hash Shuffle 在多个维度上呈现出明显不同的特征。在 IO 等待方面，Sort Shuffle 的 IO 等待时间整体较低且表现平稳，这表明其 IO 操作的效率更高。相比之下，Hash Shuffle 的 IO 等待时间明显更长，且存在显著的峰值波动，在某些特定时间点，其 IO 等待会达到非常高的水平。

关于 CPU 空闲率，Sort Shuffle 能够将 CPU 空闲率维持在相对较高的水平，大约为 30%。而 Hash Shuffle 的 CPU 空闲率则显著偏低，平均水平仅在 25% 左右，这意味着其 CPU 始终处于更为忙碌的状态。

此外，从系统总体负载水平观察，Sort Shuffle 的负载整体较低，在大部分时间内都维持在 0 到 2 的区间。与之形成鲜明对比的是，Hash Shuffle 的系统负载显著更高，其峰值负载甚至可以达到 16，是 Sort Shuffle 的 8 倍以上，这给系统带来了更大的压力。

### 内存限制

在本任务场景下，Sort Shuffle 在执行效率、稳定性以及对内存约束的适应性方面均显著优于 Hash Shuffle。在内存充足（8GB）的理想条件下，Sort Shuffle 的执行时间比 Hash Shuffle 缩短约 10.7%，其优势主要来源于更高效的 I/O 模型。Hash Shuffle 会产生大量细碎的小文件并触发频繁的随机磁盘 I/O，而 Sort Shuffle 通过排序与归并，大幅减少输出文件数量并放大单文件规模，从而充分发挥顺序读写的磁盘性能优势，提升整体吞吐量。

在内存受限环境下，两者差异进一步扩大。Hash Shuffle 对内存高度敏感，内存从 8GB 降至 512MB 后，其执行时间大幅恶化，根本原因在于缺乏主动 spill 机制，只能依赖高开销的 Full GC 回收内存，导致应用长时间暂停。相比之下，Sort Shuffle 可主动将数据溢写到磁盘，以适度的 I/O 开销换取显著降低的 GC 时间，体现出更强的内存耐受性和运行稳定性。

最后，两种 Shuffle 在 256MB 内存下均发生 OOM，表明该任务存在不可突破的最小内存门槛，实验也验证了在该场景中，512MB 是保障程序正常运行的最低可行内存配置。

### 数据倾斜

![替代文本](/picture/图片%2031.png)

**核心结论**：Hash 普遍略快但随着数据集的增大优势递减，极端倾斜是两种shuffle共同的瓶颈。

**性能差异**：在所有能够成功运行的场景中，Hash Shuffle 的执行时间普遍优于 Sort Shuffle，但优势随着数据量的增大而逐渐缩小（从约 8% 降至约 2%）。这验证了 Hash Shuffle 省去了排序开销带来的性能红利。

**稳定性瓶颈**：在 Large 数据集 + Skew 2.0 的极端倾斜场景下，两种 Shuffle 模式均告失败。这表明单纯切换 Shuffle Manager 无法解决严重的倾斜问题，系统资源（内存/网络/磁盘）的物理极限成为了主要矛盾。

**数据倾斜影响**：
- 轻度/中度倾斜（Skew 1.0）：出人意料的是，在 Medium 和 Large 数据集下，Skew 1.0 的处理速度反而比均匀分布（Skew 0.0）略快。这可能是因为 Zipf 分布导致数据集中在少数 Key 上，减少了部分随机 I/O 或对象创建的开销。
- 重度倾斜（Skew 2.0）：性能急剧下降。在 Medium 数据集下，Skew 2.0 相比 Skew 1.0 导致执行时间增加了 60%~70%。

**应用场景的选择**：在 Spark 1.6 环境下，如果作业不涉及排序需求且数据量中等，使用 Hash Shuffle 性能会更好。否则，Sort Shuffle 是大数据场景的更好选择。


## 分工
汪柔柔 工作安排协调，数据倾斜实验，README文档书写，PPT制作   25%工作量

代健坤 环境搭建，基础实验，分区基数实验，README文档书写，PPT制作   25%工作量

朱施颐 cpu与负载监测，基础实验完善，README文档书写，PPT制作   25%工作量


贺雯忆 内存限制实验，README文档书写，PPT制作   25%工作量



## 不足与展望

- 面对 Large 数据集且存在严重倾斜的场景，不要寄希望于切换 Shuffle Manager 来解决问题。必须增加资源、提高并行度或在代码层面进行抗倾斜处理。

## 遇到的问题

### 数据倾斜

选取small和large数据集时，EXECUTOR_MEM="2G"无法完成hash,large,skew=2的实验。
![替代文本](/picture/图片%2032.png)
executor_mem=4G时仍然无法完成：
![替代文本](/picture/图片%2033.png)
尝试的改进：
- 增加超时时间：让 Driver 对“卡死”的 Executor 更宽容。
- executor_mem增大到8G。


package com.sdu.spark.start

import com.google.common.base.Strings
import org.apache.spark.{SparkConf, SparkContext}

/**
  * |----------------------------------------------------------------------------------------|
    |                                                                  |----------------|    |
    |              |---------------------------------------------------|  Worker NODE   |    |
    |              |                                                   |  |----------|  |    |
    |              |                                       |-----------|  | Executor |  |    |
                   |                                       |           |  |----------|  |    |
    |              |                                       |           |----------------|    |
    |   |-----------------------|                 |-----------------|                        |
    |   |                       |  apply resource |                 |                        |
    |   | Driver(SparkContext)  |---------------> | Cluster Manager |                        |
    |   |                       |                 |                 |                        |
    |   |-----------------------|                 |-----------------|                        |
    |              |                                       |           |----------------|    |
    |              |                                       |           |  Worker NODE   |    |
    |              |                                       |           |  |----------|  |    |
    |              |                                       |-----------|  | Executor |  |    |
    |              |---------------------------------------------------|  |----------|  |    |
    |                                                                  |----------------|    |
    |----------------------------------------------------------------------------------------|
  *
  * 1: Spark集群中有四种节点:Cluster、Master、Driver和Executor,其中Driver并属于集群中单独的节点类型,运行与集群内部或独立于集群
  *
  * 2: SparkContext是Spark的主要接口,是Spark上层应用与底层实现的中转站,SparkContext在初始化过程中,主要涉及:
        1': SparkEnv[包含组件:BlockManager, MapOutputTracker, ShuffleFetcher, ConnectionManager]

        2': DAGScheduler

        3': TaskScheduler[SparkContext.createTaskScheduler() ===>> 创建TaskScheduler及SchedulerBackend]

        4': SchedulerBackend

        5': WEB-UI
  *
  * 3: 宽依赖和窄依赖
        1': 宽依赖是指父RDD的所有输出会被指定的子RDD消费,如下图:
                    |-----|            |-----|
                    | rdd | ========>  | rdd |
                    |-----|            |-----|

                    |-----|            |-----|
                    | rdd | ========>  | rdd |
                    |-----|            |-----|
        2': 窄依赖是指父RDD的所有输出会被不同的RDD消费,如下图
                    |-----|            |-----|
                    | rdd | -------->  | rdd |
                    |-----| \          |-----|
                             \
                              \------->|-----|
                                       | rdd |
                                       |-----|
        3': Spark Schedule计算RDD之间的依赖关系,并将窄依赖的RDD划分归并到同一个Stage中
  *
  *
  * Spark Cluster Manager =====>> http://spark.apache.org/docs/latest/cluster-overview.html
  *
  * @author hanhan.zhang
  * */
object SparkTaskStarter {

  def start: Unit = {

    // Spark Config
    val sparkConf = new SparkConf
    // 设置Spark Job名字[Spark UI展示]
    sparkConf.setAppName("spark.word.count")
    // 设置Master[建议用spark-submit提交,而非硬编码,参见http://spark.apache.org/docs/latest/submitting-applications.html]
    // spark.master ==>> Spark连接的Cluster的地址
    sparkConf.setMaster("local")

    // SparkContext是Spark程序的入口,用于生成RDD、在集群中广播变量等
    // Jvm中只能存在一个SparkContext实例
    val sparkContext = new SparkContext(sparkConf)

    // RDD[弹性分布式数据集]创建
    // 1: SparkContext.parallelize()
    // 2: external storage system, such as HBase, HDFS
    val sentenceData = Array("the cow jumped over the moon",
                         "the man went to the store and bought some candy",
                         "four score and seven years ago",
                         "how many apples can you eat")
    val rdd = sparkContext.parallelize[String](sentenceData)

    // RDD Operation[Transformation和Action两种]
    // Transformation ===>> 对RDD变换生成新RDD[具有惰性,并不会立即触发计算,仅有当Action触发时,Transformation才会被触发]
    // Action         ===>> 触发Job提交
    rdd.filter(sentence => !Strings.isNullOrEmpty(sentence))
       // 一对一的关系
       .map(sentence => sentence.split(" "))
       // 一对多的关系[一条记录转为多条记录]
       .flatMap(words => words.map(word => (word, 1)))
       // shuffle[repartition(如:repartition,coalesce)
       // ByKey(如:groupByKey,reduceByKey),join(如:cogroup,join)操作会导致shuffle操作]
       .reduceByKey((a, b) => a + b)
       // Spark的Action触发Job的提交,Spark的根据RDD是否为窄依赖划分Stage
       // 运行在Driver端
       .foreach(tuple => println("statistic result : [" + tuple._1 + "," + tuple._2 + "]"))

  }

  def main(args: Array[String]): Unit = {
    SparkTaskStarter.start
  }

}

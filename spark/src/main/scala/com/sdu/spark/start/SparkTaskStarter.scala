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
    // Action         ===>> RDD运行结果返回给Driver[Action运行在Driver端]
    rdd.filter(sentence => !Strings.isNullOrEmpty(sentence))
       // 一对一的关系
       .map(sentence => sentence.split(" "))
       // 一对多的关系[将一条记录转为多条记录]
       .flatMap(words => words.map(word => (word, 1)))
       // shuffle[repartition(如:repartition,coalesce),ByKey(如:groupByKey,reduceByKey),join(如:cogroup,join)操作会导致shuffle操作]
       .reduceByKey((a, b) => a + b)
       .foreach(tuple => println("statistic result : [" + tuple._1 + "," + tuple._2 + "]"))

  }

  def main(args: Array[String]): Unit = {
    SparkTaskStarter.start
  }

}

package com.sdu.spark.design.event

/**
  * sealed
      1': sealed修饰的trait或class只能够在当前的头文件中被继承

      2': 在模式匹配中,用sealed修饰目的是让Scala知道case的所有情况[缺少则会编译失败]
  *
  * case class
      1': 对象初始化可不用new[ClassName()即可产生实例]

      2': 默认实现equals和hashcode

      3': 默认可序列化[实现Serializable]

      4': 构造函数参数访问级别为public

      5': 支持模式匹配
  *
  * @author hanhan.zhang
  * */

private[design] sealed trait JobEvent

private[design] case class JobCommitEvent(jobId : Int, timestamp : String) extends JobEvent

private[design] case class JobCompleteEvent(jobId : Int, timestamp : String) extends JobEvent

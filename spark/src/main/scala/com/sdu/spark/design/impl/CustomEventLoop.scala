package com.sdu.spark.design.impl

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.sdu.spark.design.SparkEventLoop
import com.sdu.spark.design.event.{JobCommitEvent, JobCompleteEvent, JobEvent}

import scala.collection.mutable

/**
  * 生产者-消费者模型
  *
  * @author hanhan.zhang
  * */
class CustomEventLoop(name : String) extends SparkEventLoop[JobEvent](name) {

  // 事件处理[由子类实现]
  override def onReceive(event: JobEvent): Unit = {
    event match {
      case JobCommitEvent(jobId, timestamp) => {
        println("在" + timestamp + "提交任务,JobId = " + jobId)
      }
      case JobCompleteEvent(jobId, timestamp) => {
        println("在" + timestamp + "完成任务,JobId = " + jobId)
      }
    }
  }

  override def onError(ex: Exception): Unit = {
    // ignore
  }

  //
  override def onStart: Unit = {

  }
}

object CustomEventLoop {

  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // 任务队列
    val jobQueue = new mutable.Queue[Int]()

    val jobIdGenerator = new AtomicInteger(1)

    val eventLoop = new CustomEventLoop("custom-event-loop")
    eventLoop.start

    while (true) {
      if (jobQueue.isEmpty) {
        val jobId = jobIdGenerator.getAndIncrement()
        eventLoop.post(JobCommitEvent(jobId, sdf.format(new Date)))
        jobQueue.enqueue(jobId)
        TimeUnit.SECONDS.sleep(1)
      } else {
        val jobId = jobQueue.dequeue()
        eventLoop.post(JobCompleteEvent(jobId, sdf.format(new Date)))
      }
    }
  }

}
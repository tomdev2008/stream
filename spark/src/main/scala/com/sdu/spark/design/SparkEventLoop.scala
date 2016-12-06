package com.sdu.spark.design

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicBoolean

/**
  * 模拟Spark事件驱动
  *
  * @author hanhan.zhang
  * */
abstract class SparkEventLoop[E](val name : String) {

  // 事件队列
  private val eventQueue = new LinkedBlockingDeque[E]

  //
  private val stopped = new AtomicBoolean(false)

  // 事件处理现场
  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      while (!stopped.get()) {
        // 获取事件,交由子类的onReceive处理
        val event = eventQueue.take()
        try {
          onReceive(event)
        } catch {
          case ex : Exception => onError(ex)
        }
      }
    }
  }

  // 事件处理[由子类实现]
  def onReceive(event : E)

  def onError(ex : Exception)

  // 投递事件
  def post(event : E): Unit = {
    eventQueue.put(event)
  }

  //
  def onStart

  //
  def start: Unit = {
    if (stopped.get()) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    onStart
    // 启动事件线程
    eventThread.start()
  }

  def stop: Unit = {
    stopped.set(true)
    eventThread.interrupt()
    eventThread.join()
  }

}

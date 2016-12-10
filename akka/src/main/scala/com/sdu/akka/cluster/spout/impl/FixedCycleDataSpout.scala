package com.sdu.akka.cluster.spout.impl

import java.util.concurrent.atomic.AtomicInteger

import com.sdu.akka.cluster.spout.DataSpout

/**
  * @author hanhan.zhang
  * */
private[cluster] class FixedCycleDataSpout[E](dataSource : Array[E]) extends DataSpout[E]{

  private val index = new AtomicInteger(0)

  override def nextTuple: E = {
    val size = dataSource.size
    val pos = index.getAndAdd(1) % size
    dataSource(pos)
  }

}

object FixedCycleDataSpout {

  def apply[E](input : E*): FixedCycleDataSpout[E] = {
    val dataSource = Array(input).asInstanceOf[Array[E]]
    new FixedCycleDataSpout[E](dataSource)
  }

}


package com.sdu.akka.cluster.spout

/**
  * @author hanhan.zhang
  * */
abstract class DataSpout[E] {

  def nextTuple : E

}

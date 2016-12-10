package com.sdu.akka.cluster.bolt

/**
  * @author hanhan.zhang
  * */
abstract class DataBolt[E, R] {

  def execute(data : E) : R

}

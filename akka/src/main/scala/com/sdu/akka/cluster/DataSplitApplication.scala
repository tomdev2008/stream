package com.sdu.akka.cluster

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.cluster.actor.impl.DataEmitActor
import com.sdu.akka.cluster.spout.impl.FixedCycleDataSpout
import com.typesafe.config.ConfigFactory

/**
  * @author hanhan.zhang
  * */
object DataSplitApplication {

  def main(args: Array[String]): Unit = {
    // 节点角色
    val nodeRole = "dataSplit"
    // TCP监听端口
    val ports = Array("2755", "2756")
    // 创建Config对象
    ports.foreach(port => {
      val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $port")
                                .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [$nodeRole]"))
                                .withFallback(ConfigFactory.load("cluster/application.conf"))
      // 创建ActorSystem
      val dataSplitActorSystem = ActorSystem("DataSplitProcessActorSystem", config)
      // 创建EmitActor
      dataSplitActorSystem.actorOf(Props(DataEmitActor(FixedCycleDataSpout[String]("1"))), s"data-split-actor-$port")
    })
  }

}

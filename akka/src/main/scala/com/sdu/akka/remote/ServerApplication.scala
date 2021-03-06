package com.sdu.akka.remote

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.remote.actor.ServerActor
import com.typesafe.config.ConfigFactory

/**
  * @author hanhan.zhang
  * */
object ServerApplication {

  def main(args: Array[String]): Unit = {
    // ActorSystem[默认读取classpath下的application.conf配置,AKKA支持json,properties,conf三种格式配置]
    val remoteServerSystem = ActorSystem("remote-server-system", ConfigFactory.load("remote/application.conf").getConfig("RemoteServer"))

    remoteServerSystem.log.info("remote-server-system started !")

    // 创建Actor
    remoteServerSystem.actorOf(Props(ServerActor()), "remote-actor-server")
  }

}

package com.sdu.akka.start

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.start.remote.RemoteActorServer
import com.typesafe.config.ConfigFactory

/**
  * @author hanhan.zhang
  * */
object RemoteServerApplication {

  def main(args: Array[String]): Unit = {
    // ActorSystem[默认读取classpath下的application.conf配置,AKKA支持json,properties,conf三种格式配置]
    val remoteServerSystem = ActorSystem("remote-server-system", ConfigFactory.load().getConfig("RemoteServer"))

    remoteServerSystem.log.info("remote-server-system started !")

    // 创建Actor
    remoteServerSystem.actorOf(Props(new RemoteActorServer), "remote-actor-server")
  }

}

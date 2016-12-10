package com.sdu.akka.remote

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.remote.actor.ClientActor
import com.sdu.akka.remote.event.ActorStartEvent
import com.typesafe.config.ConfigFactory

object ClientApplication {

  def main(args: Array[String]): Unit = {
    // ActorSystem
    val clientActorSystem = ActorSystem("client-actor-system", ConfigFactory.load("remote/application.conf").getConfig("RemoteClient"))

    // 获取远程Actor[akka.<protocol>://<actor system>@<hostname>:<port>/<actor path>]
    val path = "akka.tcp://remote-server-system@127.0.0.1:2552/user/remote-actor-server"

//    val remoteServerActorRef = clientActorSystem.actorOf(Props(classOf[RemoteActorServer], path), "remote-actor-server")
//    remoteServerActorRef ! HeartBeatMsg("1", "2")

    // 创建Actor
    val clientActor = clientActorSystem.actorOf(Props(ClientActor(path)), "client-actor")

    clientActor ! ActorStartEvent("client-actor", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))

  }

}

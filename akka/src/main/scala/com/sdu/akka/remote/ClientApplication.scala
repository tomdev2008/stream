package com.sdu.akka.remote

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.remote.actor.ClientActor
import com.sdu.akka.start.msg.ActorStart
import com.typesafe.config.ConfigFactory

object ClientApplication {

  def main(args: Array[String]): Unit = {
    // ActorSystem
    val clientActorSystem = ActorSystem("client-actor-system", ConfigFactory.load().getConfig("RemoteClient"))

    // 获取远程Actor[akka.<protocol>://<actor system>@<hostname>:<port>/<actor path>]
    val path = "akka.tcp://remote-server-system@127.0.0.1:2552/user/remote-actor-server"

//    val remoteServerActorRef = clientActorSystem.actorOf(Props(classOf[RemoteActorServer], path), "remote-actor-server")
//    remoteServerActorRef ! HeartBeatMsg("1", "2")

    // 创建Actor
    val clientActor = clientActorSystem.actorOf(Props(new ClientActor(path)), "client-actor")

    clientActor ! ActorStart("client-actor", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))

  }

}

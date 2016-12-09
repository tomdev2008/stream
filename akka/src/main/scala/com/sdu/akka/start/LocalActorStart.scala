package com.sdu.akka.start

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{ActorSystem, Props}
import com.sdu.akka.start.actor.LocalActor
import com.sdu.akka.start.msg.HeartBeatMsg

/**
  * @author hanhan.zhang
  * */
object LocalActorStart {

  def main(args: Array[String]): Unit = {
    val SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // 创建ActorSystem
    val actorSystem = ActorSystem("local-actor-system")

    // 创建Actor并获取引用
    val localActorRef = actorSystem.actorOf(Props(new LocalActor), "local-actor")

    // 向LocalActor发送消息
    localActorRef ! HeartBeatMsg("127.0.0.1", SDF.format(new Date()))

    // 关闭应用
    actorSystem.terminate()
  }

}

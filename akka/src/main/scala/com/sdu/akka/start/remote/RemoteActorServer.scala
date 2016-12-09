package com.sdu.akka.start.remote

import akka.actor.{Actor, ActorLogging}
import com.sdu.akka.start.msg.HeartBeatMsg

/**
  * AKKA Routing[解决点对点通信,官方文档:http://doc.akka.io/docs/akka/current/scala/remoting.html]
  *
  * @author hanhan.zhang
  * */
class RemoteActorServer extends Actor with ActorLogging {

  override def receive: Receive = {
    case HeartBeatMsg(fromIp, sendTime) => {
      log.info("receive heart beat from : ", (fromIp, sendTime))
    }
    case _ => log.info("unknown actor message !")
  }

}

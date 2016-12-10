package com.sdu.akka.remote.actor

import akka.actor.{Actor, ActorLogging}
import com.sdu.akka.remote.event.HeartBeatEvent

/**
  * AKKA Routing[解决点对点通信,官方文档:http://doc.akka.io/docs/akka/current/scala/remoting.html]
  *
  * @author hanhan.zhang
  * */
class RemoteServerActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case HeartBeatEvent(fromIp, sendTime) => {
      log.info("receive heart beat from : ", (fromIp, sendTime))
    }
    case _ => log.info("unknown actor message !")
  }

}

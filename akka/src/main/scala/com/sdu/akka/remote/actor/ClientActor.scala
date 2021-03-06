package com.sdu.akka.remote.actor

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{Actor, ActorLogging}
import com.sdu.akka.remote.event.{ActorStartEvent, HeartBeatEvent}

class ClientActor(remotePath : String) extends Actor with ActorLogging{

  // 远端Actor引用
  private val remoteActorRef = context.actorSelection(remotePath)

  private val SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def receive: Receive = {
    case ActorStartEvent(name, startTime) => {
      log.info("actor name : " + name + ", started at : " + startTime)
      val scheduledExecutorService = Executors.newScheduledThreadPool(1)
      scheduledExecutorService.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          remoteActorRef ! HeartBeatEvent("127.0.0.1", SDF.format(new Date()))
        }
      }, 10, 10, TimeUnit.SECONDS)
    }
    case _ => {
      log.info("unknown actor message !")
    }
  }

}

object ClientActor {

  def apply(remotePath: String): ClientActor = new ClientActor(remotePath)

}
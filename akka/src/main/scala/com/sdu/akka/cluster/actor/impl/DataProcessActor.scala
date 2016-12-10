package com.sdu.akka.cluster.actor.impl

import akka.actor.{ActorPath, Terminated}
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp, UnreachableMember}
import com.sdu.akka.cluster.actor.ClusterNodeRoleActor
import com.sdu.akka.cluster.bolt.DataBolt
import com.sdu.akka.cluster.event.{DataEmitEvent, RegisterEvent}

/**
  * @author hanhan.zhang
  * */
class DataProcessActor[E, R](subscribeActor : ActorPath, dataBolt : DataBolt[E, R]) extends ClusterNodeRoleActor {

  override def receive: Receive = {
    // 节点Join
    case MemberUp(member) => {
      log.info(s"process member ${member.address} up")
    }
    case UnreachableMember(member) => {
      log.info(s"member ${member.address} detected as unreachable")
    }
    case MemberRemoved(member, previousStatus) => {
      log.info(s"member ${member.address} removed after $previousStatus")
    }
    case RegisterEvent => {
      // 监控发送注册消息的ActorRef,如果对应的Actor终止,会发送Terminated消息
      context.watch(sender())
      // 加入下游消费Actor集合
      consumerActorRefs :+ sender()
      log.info("register actor {}", sender().path.name)
    }
    // 注册的Actor终止事件
    case Terminated(actorRef) => {
      // 删除注册的Actor
      consumerActorRefs.filterNot(_ == actorRef)
    }
    case DataEmitEvent(data) => executeAndEmit(data)
    case _ => {
      log.info("unknown event")
    }
  }

  // 数据处理并发送到下游
  private def executeAndEmit(data : Any): Unit = {
    try {
      val inputData = data.asInstanceOf[E]
      val result = dataBolt.execute(inputData)
      consumerActorRefs.foreach(actorRef => actorRef ! DataEmitEvent(result))
    } catch {
      case ex : Exception => log.error(s"error input data type : ${data.getClass} and except input data type : $classOf[E] ")
    }
  }

}

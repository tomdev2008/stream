package com.sdu.akka.cluster.actor.impl

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Terminated
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp, UnreachableMember}
import com.sdu.akka.cluster.actor.ClusterNodeRoleActor
import com.sdu.akka.cluster.event.{ActorStart, DataEmitEvent, RegisterEvent}
import com.sdu.akka.cluster.spout.DataSpout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool

/**
  * 数据源
  *
  * @author hanhan.zhang
  * */
class DataEmitActor[E](dataSpout: DataSpout[E]) extends ClusterNodeRoleActor{

  // 隐式转换
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

  private val stopped = new AtomicBoolean(false)

  override def receive: Receive = {
    // 节点Join
    case MemberUp(member) => {
      log.info(s"new member ${member.address} up")
    }
    case UnreachableMember(member) => {
      log.info(s"member ${member.address} detected as unreachable")
    }
    case MemberRemoved(member, previousStatus) => {
      log.info(s"member ${member.address} removed after $previousStatus")
    }
    // 启动Actor
    case ActorStart => emit
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
    case _ => {
      log.info("unknown event")
    }
  }

  // 发送数据
  def emit(): Unit = {
    context.system.scheduler.schedule(0 second, 1 second, new Runnable {
      override def run(): Unit = {
        consumerActorRefs.foreach(actorRef => {
          val data = dataSpout.nextTuple
          actorRef ! DataEmitEvent(data)
        })
      }
    })
  }

  def stop(): Unit = {
    stopped.compareAndSet(false, true)
  }
}

object DataEmitActor {

  def apply[E](dataSpout: DataSpout[E]): DataEmitActor[E] = new DataEmitActor(dataSpout)

}
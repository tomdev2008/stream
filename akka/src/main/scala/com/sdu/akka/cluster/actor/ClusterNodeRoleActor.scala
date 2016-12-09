package com.sdu.akka.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorPath}
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.remote.ContainerFormats.ActorRef
import com.sdu.akka.cluster.event.RegisterEvent

/**
  *
  * @author hanhan.zhang
  * */
abstract class ClusterNodeRoleActor extends Actor with ActorLogging {

  // 集群对象
  val cluster = Cluster(context.system)

  // 依赖该Actor的下游ActorRef
  val consumerActorRefs = IndexedSeq.empty[ActorRef]

  // Actor启动前订阅集群事件
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
                      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def register(member : Member, createPath: (Member) => ActorPath): Unit = {
    val actorPath = createPath(member)
    val selection = context.actorSelection(actorPath)
    selection ! RegisterEvent
  }
}

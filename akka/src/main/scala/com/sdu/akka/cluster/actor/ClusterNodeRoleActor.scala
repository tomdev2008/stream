package com.sdu.akka.cluster.actor

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, RootActorPath}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.cluster.{Cluster, Member}
import com.sdu.akka.cluster.event.RegisterEvent

/**
  *
  * @author hanhan.zhang
  * */
abstract class ClusterNodeRoleActor extends Actor with ActorLogging {

  // 集群对象
  val cluster = Cluster(context.system)

  // 依赖该Actor的下游ActorRef
  var consumerActorRefs = IndexedSeq.empty[ActorRef]

  // Actor启动前订阅集群事件
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
                      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  // member = 集群节点, createPath = 获得订阅节点的ActorPath
  def register(member : Member, createPath: (Member) => ActorPath): Unit = {
    val actorPath = createPath(member)
    val selection = context.actorSelection(actorPath)
    selection ! RegisterEvent
  }

  // 订阅Actor的ActorPath
  def getSubscribeActorPath(member: Member, subscribeActor : String): Unit = {
    RootActorPath(member.address) / "user" / subscribeActor
  }
}

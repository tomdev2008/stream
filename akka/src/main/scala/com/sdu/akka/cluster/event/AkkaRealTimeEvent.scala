package com.sdu.akka.cluster.event

import akka.actor.ActorPath
import akka.cluster.Member

private[cluster] sealed trait AkkaRealTimeEvent

// 启动
private[cluster] case class ActorStart() extends AkkaRealTimeEvent

// 注册
private[cluster] case class RegisterEvent(member: Member, registerActorPath : ActorPath) extends AkkaRealTimeEvent

// 数据发送
private[cluster] case class DataEmitEvent(data : Any) extends AkkaRealTimeEvent
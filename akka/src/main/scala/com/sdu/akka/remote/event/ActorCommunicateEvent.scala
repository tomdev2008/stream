package com.sdu.akka.remote.event

private[remote] sealed trait ActorCommunicateEvent

private[remote] case class ActorStartEvent(name : String, startTime: String) extends ActorCommunicateEvent

private[remote] case class HeartBeatEvent(fromIp : String, sendTime : String) extends ActorCommunicateEvent



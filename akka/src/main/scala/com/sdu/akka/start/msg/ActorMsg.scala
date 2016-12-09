package com.sdu.akka.start.msg

private[start] sealed trait ActorMsg

// 心跳[保持长连接]
private[start] case class HeartBeatMsg(fromIp : String, sendTime : String) extends ActorMsg

private[start] case class ActorStart(name : String, startTime: String) extends ActorMsg

// 关闭
private[start] case class ShutDown(waitSec : Int) extends ActorMsg

// 任务提交
private[start] case class JobSubmit(jobId : String, time : String) extends ActorMsg

// 任务完成
private[start] case class JobComplete(jobId : String, time : String) extends ActorMsg

// 任务异常
private[start] case class JobException(jobId : String, exp : Exception) extends ActorMsg
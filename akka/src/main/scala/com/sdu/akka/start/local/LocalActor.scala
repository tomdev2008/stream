package com.sdu.akka.start.local

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging}
import com.sdu.akka.start.msg.{HeartBeatMsg, JobComplete, JobException, JobSubmit, ShutDown}

/**
  * 1: Actor是AKKA的核心概念,封装了状态和行为,Actor之间通过消息的方式进行通信,每个Actor都有自己的收件箱[Mailbox]
  *
  * 2: 在AKKA中,ActorSystem是一个重量级结构,具有分层结构:
       Actor能够管理某个特定的函数,它可能希望将task分解为更小的多个子task,这样它就需要创建多个子Actor[Child Actors],
       并监督这些子Actor处理任务的进度等详细情况,实际上这个Actor创建了Supervisor来监督管理子Actor执行拆分后的多个子task,
       如果子Actor执行子task失败,则需要向Supervisor发送消息说明处理子task失败

      Note:
      一个Actor能且仅能有一个Supervisor,就是创建它的那个Actor,基于被监控任务的性质和失败的性质,一个Supervisor可以选择执行
      如下操作选择：
      1': 重新开始一个子Actor并保持它内部的状态
      2': 重启一个子Actor并清除它内部的状态
      3': 终止一个子Actor
      4': 扩大失败的影响从而使这个子Actor失败
  *
  * 3: ActorSystem在创建过程中,至少启动3个Actor,如下图:
                        |-------|
                |-------|   /   |--------|
                |       |-------|        |
                |                        |
            |------|                 |--------|
            | User |                 | System |
            |------|                 |--------|

      ActorSystem的监督范围:
      1': “/”路径            ===>> 通过根路径可以搜索到所有的Actor
      2': “/user”路径        ===>> 用户创建的Top-Level Actor在该路径下面,通过调用ActorSystem.actorOf来实现Actor的创建
      3': “/system”路径      ===>> 系统创建的Top-Level Actor在该路径下面
      4': “/deadLetters”路径 ===>> 消息被发送到已经终止,或者不存在的Actor,这些Actor都在该路径下面
      5': “/temp”路径        ===>> 系统临时创建的Actor在该路径下面
      6': “/remote”路径      ===>> 改路径下存在的Actor,它们的Supervisor都是远程Actor的引用

  *
  *
  * @author hanhan.zhang
  * */
class LocalActor extends Actor with ActorLogging{

  override def receive = {

    case HeartBeatMsg(fromIp, sendTime) => {
      log.info("receive heart beat : " + (fromIp, sendTime))
    }
    case JobSubmit(jobId, time) => {
      log.info("submit job : " + (jobId, time))
    }
    case JobComplete(jobId, time) => {
      log.info("complete job : " + (jobId, time))
    }
    case JobException(jobId, exp) => {
      log.info("execute job " + jobId + " occur exception : " + exp.getMessage)
    }
    case ShutDown(waitSec) => {
      log.info("shut down actor after " + waitSec + " second !")
      TimeUnit.SECONDS.sleep(waitSec)
      context.system.terminate()
    }
  }
}

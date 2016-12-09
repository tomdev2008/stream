package com.sdu.akka.cluster

/**
  * Akka集群支持去中心化的基于P2P的集群服务,不存在单点故障问题[通过Gossip协议来实现]
  *
  * 对于集群成员的状态,Akka提供了一种故障检测机制,能够自动发现出现故障而离开集群的成员节点并通过事件驱动的方式将状态传播到整个集群中的节点
  *
  * Akka支持在节点加入集群的时设置成员的角色,通过角色划分可将Akka集群的系统划分为多个逻辑独立的子系统,每个子系统处理各自业务逻辑
  *
  * @author hanhan.zhang
  * */
object ClusterApplication {

}

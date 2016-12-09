package com.sdu.akka.cluster.event

private[cluster] sealed trait ClusterEvent

private[cluster] case class RegisterEvent() extends ClusterEvent
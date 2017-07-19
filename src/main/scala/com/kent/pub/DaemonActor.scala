package com.kent.pub
import com.kent.pub.ActorTool.ActorInfo
import com.kent.pub.ActorTool.ActorType._

abstract class DaemonActor extends ActorTool{
  override val actorType = DAEMO
}
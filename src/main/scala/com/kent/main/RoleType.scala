package com.kent.main
/**
 * 集群角色类型
 */
object RoleType {
  val MASTER = "master"
  val MASTER_ACTIVE = "master-active"
  val MASTER_STANDBY = "master-standby"
  val WORKER = "worker"
  val HTTP_SERVER = "http-server"
}
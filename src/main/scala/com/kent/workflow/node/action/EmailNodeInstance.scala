package com.kent.workflow.node.action

class EmailNodeInstance(override val nodeInfo: EmailNode) extends ActionNodeInstance(nodeInfo) {
  def execute(): Boolean = {
    actionActor.sendMailMsg(null, "【Akkaflow】通知", nodeInfo.htmlContent)
    true
  }
  
  def kill(): Boolean = {
    true
  }
}
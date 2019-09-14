package com.kent.workflow.node.action.transfer.target.db

import com.kent.pub.io.FileLink
import com.kent.workflow.node.action.ActionNodeInstance

class HdfsTarget(fl: FileLink, delimited: String, path: String, isPreDel: Boolean,
                 preCmd: String, afterCmd: String,
                 instanceId: String, actionName: String, actionInstance: ActionNodeInstance) extends GenericFileTarget (fl, delimited, path, isPreDel, preCmd, afterCmd, instanceId, actionName, actionInstance){

}
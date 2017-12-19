package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.ActionNode
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.node.NodeInstance
import org.json4s.jackson.JsonMethods

class JoinNodeInstance(override val nodeInfo: JoinNode) extends ControlNodeInstance(nodeInfo) {
  
  override def ifCanExecuted(wfi: WorkflowInstance): Boolean = {
		var isExcecuted = true
		//找到下一节点是该join节点的节点，并判断是否所有都success
    wfi.nodeInstanceList.filter { _.isInstanceOf[ActionNodeInstance] }.foreach { x => 
      if(x.asInstanceOf[ActionNodeInstance].nodeInfo.ok == nodeInfo.name && x.getStatus() == SUCCESSED){
      }else if(x.asInstanceOf[ActionNodeInstance].nodeInfo.error == nodeInfo.name && x.getStatus() == FAILED){
      }else if(x.asInstanceOf[ActionNodeInstance].nodeInfo.error == nodeInfo.name || x.asInstanceOf[ActionNodeInstance].nodeInfo.ok == nodeInfo.name){
        isExcecuted = false
      }
    }
     wfi.nodeInstanceList.filter { _.isInstanceOf[ControlNodeInstance]}.foreach { n =>
       if(n.isInstanceOf[JoinNodeInstance] ){
         var b = n.asInstanceOf[JoinNodeInstance]
         if(b.nodeInfo.to == nodeInfo.name && b.getStatus() != SUCCESSED) isExcecuted = false
       }else if(n.isInstanceOf[StartNodeInstance] ){
         var b = n.asInstanceOf[StartNodeInstance]
         if(b.nodeInfo.to == nodeInfo.name && b.getStatus() != SUCCESSED) isExcecuted = false
       }else if(n.isInstanceOf[ForkNodeInstance] ){
         var b = n.asInstanceOf[ForkNodeInstance]
         if(b.nodeInfo.pathList.contains(nodeInfo.name) && b.getStatus() != SUCCESSED) isExcecuted = false
       }
   }
   isExcecuted 
  }
  
  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = wfi.nodeInstanceList.filter { _.nodeInfo.name == nodeInfo.to }.toList
}

object JoinNodeInstance {
  def apply(joinNode: JoinNode): JoinNodeInstance = new JoinNodeInstance(joinNode)
}
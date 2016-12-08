package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util

class JoinNodeInfo(name: String) extends ControlNodeInfo(name)  {
  var to: String = _

  override def deepClone(): JoinNodeInfo = {
    val jn = JoinNodeInfo(name)
    jn.to = to
    this.deepCloneAssist(jn)
    jn
  }

  override def createInstance(workflowInstanceId: String): JoinNodeInstance = {
    val jni = JoinNodeInstance(this)
    jni.id = workflowInstanceId
    jni
  }

  def save(implicit conn: Connection): Boolean = {
    val insertStr = s"""
      insert into node 
      values('${name}',0,'${getClass.getName}','{"to":"${to}"}','${workflowId}','${desc}')
      """
    val updateStr = s"""
      update node set type = '${getClass.getName}',is_action = 0,
                      content = '{"to":"${to}"}', workflow_id = '${workflowId}',
                      description = '${desc}'
                      where name = '${name}'
      """
     
    if(this.getEntity.isEmpty){
    	executeSql(insertStr)     
    }else{
      executeSql(updateStr)
    }
  }

  def delete(implicit conn: Connection): Boolean = {
    executeSql(s"delete from node where name='${name}'")
  }

  def getEntity(implicit conn: Connection): Option[JoinNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content->"$.to" nto
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            this.to = Util.remove2endStr(rs.getString("nto"), "\"")
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object JoinNodeInfo {
  def apply(name: String): JoinNodeInfo = new JoinNodeInfo(name)
  def apply(node: scala.xml.Node): JoinNodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): JoinNodeInfo = {
	  val nameOpt = node.attribute("name")
	  val toOpt = node.attribute("to")
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在join未配置name属性")
    }
    val jn = JoinNodeInfo(nameOpt.get.text) 
    jn.to = toOpt.get.text
    jn
  }
}
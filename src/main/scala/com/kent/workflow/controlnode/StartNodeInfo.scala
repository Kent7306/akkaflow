package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import java.sql.ResultSet

class StartNodeInfo(name: String) extends ControlNodeInfo(name){
  var to: String = _

  override def deepClone(): StartNodeInfo = {
    val sn = StartNodeInfo(this.name, this.to)
    super.deepCloneAssist(sn)
    sn
  }

  override def createInstance(workflowInstanceId: String): StartNodeInstance = {
    val sni = StartNodeInstance(this)
    sni.id = workflowInstanceId
    sni
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

  def getEntity(implicit conn: Connection): Option[StartNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content->"$.to" nto
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            this.to = rs.getString("nto").replaceAll("\"", "")
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object StartNodeInfo {
  def apply(name: String, to: String): StartNodeInfo = {
    val sn = new StartNodeInfo(name)
    sn.to = to
    sn
  }
  def apply(node: scala.xml.Node):StartNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): StartNodeInfo = {
      val nameOpt = node.attribute("name")
      val toOpt = node.attribute("to")
      
      if(nameOpt == None){
        throw new Exception("节点<start/>未配置name属性")
      }else if(toOpt == None){
        throw new Exception("节点<start/>未配置opt属性")
      }
      val sn = StartNodeInfo(nameOpt.get.text, toOpt.get.text)
      sn
  }
}
package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util

class KillNodeInfo(name: String) extends ControlNodeInfo(name) {
  var msg: String = _

  def deepClone(): KillNodeInfo = {
    val kn = KillNodeInfo(this.name)
    kn.msg = msg
    this.deepCloneAssist(kn)
    kn
  }

  override def createInstance(workflowInstanceId: String): KillNodeInstance = {
    val kni = KillNodeInstance(this)
    kni.id = workflowInstanceId
    kni
  }

  def save(implicit conn: Connection): Boolean = {
    val insertStr = s"""
      insert into node 
      values('${name}',0,'${getClass.getName}','{"msg":"${msg}"}','${workflowId}','${desc}')
      """
    val updateStr = s"""
      update node set type = '${getClass.getName}',is_action = 0,
                      content = '{"msg":"${msg}"}', workflow_id = '${workflowId}',
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

  def getEntity(implicit conn: Connection): Option[KillNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content->"$.msg" msg
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            this.msg = Util.remove2endStr(rs.getString("msg"), "\"")
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object KillNodeInfo {
  def apply(name: String): KillNodeInfo = new KillNodeInfo(name)
  def apply(node: scala.xml.Node): KillNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): KillNodeInfo = {
      val nameOpt = node.attribute("name")
      val msg = (node \ "message").text
      if(nameOpt == None){
        throw new Exception("节点<kill/>未配置name属性")
      }
      val kn = KillNodeInfo(nameOpt.get.text)
      kn.msg = msg
      kn
  }
}
package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet

class EndNodeInfo(name: String) extends ControlNodeInfo(name){
  def deepClone(): EndNodeInfo = {
    val en = EndNodeInfo(name)
    this.deepCloneAssist(en)
    en
  }

  override def createInstance(workflowInstanceId: String): EndNodeInstance = {
    val eni = EndNodeInstance(this)
    eni.id = workflowInstanceId: String
    eni
  }

   def save(implicit conn: Connection): Boolean = {
    val insertStr = s"""
      insert into node 
      values('${name}',0,'${getClass.getName}','{}','${workflowId}','${desc}')
      """
    val updateStr = s"""
      update node set type = '${getClass.getName}',is_action = 0,
                      content = '{}', workflow_id = '${workflowId}',
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

  def getEntity(implicit conn: Connection): Option[EndNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object EndNodeInfo {
  def apply(name: String): EndNodeInfo = new EndNodeInfo(name)
  def apply(node: scala.xml.Node): EndNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): EndNodeInfo = {
    val nameOpt = node.attribute("name")
    if(nameOpt == None){
      throw new Exception("节点<end/>未配置name属性")
    }
    val en = EndNodeInfo(nameOpt.get.text)
    en
  }
}
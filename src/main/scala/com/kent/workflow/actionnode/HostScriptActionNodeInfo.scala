package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods

class HostScriptActionNodeInfo(name: String) extends ActionNodeInfo(name) {
  var host: String = _
  var script: String = _

  def deepClone(): HostScriptActionNodeInfo = {
    val fn = HostScriptActionNodeInfo(name)
    deepCloneAssist(fn)
    fn
  }
  def deepCloneAssist(hn: HostScriptActionNodeInfo): HostScriptActionNodeInfo = {
    super.deepCloneAssist(hn)
    hn.host = host
    hn.script = script
    hn
  }

  def createInstance(workflowInstanceId: String): HostScriptActionNodeInstance = {
    val hsani = HostScriptActionNodeInstance(this)
    hsani.id = workflowInstanceId
    hsani
  }

  def save(implicit conn: Connection): Boolean = {
    val insertStr = s"""
      insert into node 
      values('${name}',0,'${getClass.getName}','{"host":"${host}","script":"${script}"}','${workflowId}','${desc}')
      """
    val updateStr = s"""
      update node set type = '${getClass.getName}',is_action = 0,
                      content = '{"host":"${host}","script":"${script}"}', workflow_id = '${workflowId}',
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

  def getEntity(implicit conn: Connection): Option[HostScriptActionNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content->"$.host" host,content->"$.script" script
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) =>{
          if(rs.next()){
            this.host = rs.getString("host")
            this.script = rs.getString("script")
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object HostScriptActionNodeInfo {
  def apply(name: String): HostScriptActionNodeInfo = new HostScriptActionNodeInfo(name)
  def apply(name:String, node: scala.xml.Node): HostScriptActionNodeInfo = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): HostScriptActionNodeInfo = {
    val host = (node \ "host")(0).text
    val script = (node \ "script")(0).text
    
	  val hsan = HostScriptActionNodeInfo(name)
	  hsan.host = host
	  hsan.script = script
	  hsan
  }
}
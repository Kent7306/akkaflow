package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JString

class ForkNodeInfo(name: String) extends ControlNodeInfo(name){
  var pathList: List[String] = List()

  def deepClone(): ForkNodeInfo = {
    val fn = ForkNodeInfo(name)
    fn.pathList = pathList.map (_.toString()).toList
    this.deepCloneAssist(fn)
    fn
  }

  override def createInstance(workflowInstanceId: String): ForkNodeInstance = {
    val fni = ForkNodeInstance(this)
    fni.id = workflowInstanceId
    fni
  }

  def save(implicit conn: Connection): Boolean = {
    
    
    val insertStr = s"""
      insert into node 
      values('${name}',0,'${getClass.getName}','{"paths":"${this.pathList.mkString("[", "," ,"]")}"}','${workflowId}','${desc}')
      """
    val updateStr = s"""
      update node set type = '${getClass.getName}',is_action = 0,
                      content = '{"paths":"${this.pathList.mkString("[", "," ,"]")}"}', workflow_id = '${workflowId}',
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

  def getEntity(implicit conn: Connection): Option[ForkNodeInfo] = {
    val queryStr = """
         select name,type,is_action,workflow_id,description, content
         from node where name='"""+name+"""' and workflow_id = '"""+workflowId+""""'
                    """
    querySql(queryStr, (rs: ResultSet) => {
          if(rs.next()){
            val json = JsonMethods.parse(rs.getString("content"))
            this.pathList = (json \ "paths" \\ classOf[JString]).asInstanceOf[List[String]]
            this.desc = rs.getString("description")
            this.workflowId = rs.getString("workflow_id")
            this
          }else{
            null
          }
      })
  }
}

object ForkNodeInfo {
  def apply(name: String): ForkNodeInfo = new ForkNodeInfo(name)
  def apply(node: scala.xml.Node): ForkNodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): ForkNodeInfo = {
	  val nameOpt = node.attribute("name")
		val pathList = (node \ "path").map { x => x.attribute("to").get.text }.toList
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在fork未配置name属性")
    }else if(pathList.size <= 0){
       throw new Exception("[fork] "+nameOpt.get.text+":-->[error]:未配置path子标签")
    }
    val fn = ForkNodeInfo(nameOpt.get.text)
    fn.pathList = pathList
    fn
  }
}
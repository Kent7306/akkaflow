package com.kent.workflow.node.action

import com.kent.util.Util._

/** usage:
表注释配置节点（元数据配置节点）
给指定的库表添加注释，可以通过设置来源库表，由来源的库表有效字段自动填充目标库表，简化注释配置；再用显式声明的注释来覆盖默认注释。
属性：
table: 必填，目标表，用以添加注释
db-link: 必填，指定数据库连接
from: 可选，来源于哪些表，可以根据字段名称，把源表的字段注释拿过来默认填充，多个表用逗号分隔
子标签：
column-comment: 可选，字段注释标签，显示指定字段的注释，若配置源表，默认为源表同字段的注释
    属性：必填，name，字段名称，标签内容为字段注释
table-comment: 可选，表注释标签，默认不配置时，工作流说明即为该表的注释

<metadata table="ods.thes_order_i" db-link="local_oracle" from="ods.order_sku_i,ods.kuaigoes">
    <column-comment name="order_id">is_go 冗余字段，用于xsxx</column-comment>
    <column-comment name="ds">happy 1:->xxx,2:->xxxx</column-comment>
    <column-comment name="outstore_id">bus_type 业务类型</column-comment>
    <column-comment name="physisit2">ds 分区字段</column-comment>
    <table-comment>dfrerdswer</table-comment>
</metadata>
 */

class MetadataNode(name: String) extends ActionNode(name) {
  var colCommentMap: Map[String, String] = Map()
  var tableCommentOpt: Option[String] = None
  var dbLinkName: String = _
  var table: String = _
  //来自哪个表，自动补全
  var fromTables = List[String]()
  
  def toJsonString(): String = {
    "{}"
  }
}

object MetadataNode {
  def apply(name: String): MetadataNode = new MetadataNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): MetadataNode = {
	  val node = MetadataNode(name)
	  //字段注释
	  node.colCommentMap = (xmlNode \ "column-comment").map{ cnode =>
	    val nameOpt = cnode.attribute("name")
	    if(nameOpt.isEmpty) throw new Exception("存在column-comment未配置name（字段名）")
	    (nameOpt.get.text, cnode.text)
	  }.toMap
	  //表注释
	  node.tableCommentOpt = if((xmlNode \ "table-comment").size == 0){
	    None
	  }else{
	    Some((xmlNode \ "table-comment")(0).text)
	  }
	  //来自哪个表，自动补全
	  node.fromTables = xmlNode.attribute("from") match {
	    case Some(fromTableSeq) => fromTableSeq.text.split(",").map(_.trim()).toList
	    case None => List[String]()
	  }
	  //表名
	  node.table = xmlNode.attribute("table") match {
	    case Some(tableSeq) => tableSeq.text
	    case None => throw new Exception("未配置table属性")
	  }
	  
	  //db-link
	  node.dbLinkName = xmlNode.attribute("db-link") match {
	    case Some(dbLinkSeq) => dbLinkSeq.text
	    case None => throw new Exception("未配置db-link")
	  }
	  node
  }
  
  case class SelfColumn(name: String, colType: String, var comment: String)
}
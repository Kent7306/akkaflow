package com.kent.test

import java.io.File
import com.kent.coordinate.ParamHandler
import java.util.Date

object Test2 extends App {
  val str = """
    <work-flow name="wf_join_order_and_item" creator="张三" mail-level = "W_FAILED,W_KILLED"
        mail-receivers="15018735011@163.com,492005267@qq.com" dir="/example" desc="导入item数据">

<coordinator>
    <depend-list>
        <workflow name="wf_import_item"></workflow> 
        <workflow name="wf_import_order"></workflow>
    </depend-list> 
    <param-list>
        <param name="stime" value="${time.today|yyyy-MM-dd hh:mm}"/>
        <param name="stadate" value="${time.today|yyyy-MM-dd hh}"/>  
    </param-list>
</coordinator>

        <start name="start" to="join_sql" />
        <action name="join_sql" desc = "join两个表">
            <sql type="MYSQL" jdbc-url="jdbc:mysql://localhost:3306/wf?useSSL=false" username="root" password="root">
                  drop table if exists example_order_item_detail;
                  create table example_order_item_detail as
                  select a.ds,a.orderid,username,status,goods_name
                  from example_order a join example_item b
                  on a.ds = b.ds and a.orderid = b.orderid;

            </sql>
            <ok to="data_monitor"/>
        </action>  
        <action name="data_monitor" desc = "监测日志文件">
            <data-monitor category="mysql" source-name="example_order_item_detail" is-saved="true" is-exceed-error="true" time-mark="sdf">
                <source type="MYSQL" jdbc-url="jdbc:mysql://localhost:3306/wf?useSSL=false" username="root" password="root">
                    select count(1) from example_order_item_detail where ${param:stadate}
                </source>
                <min-threshold type="NUM">10</min-threshold>
                <max-threshold type="MYSQL" jdbc-url="jdbc:mysql://localhost:3306/wf?useSSL=false" username="root" password="root">
                    select count(1)+10 from example_order_item_detail
                </max-threshold>
            </data-monitor>  
            <ok to="end"/>
        </action>

        <end name="end"/>
</work-flow>
    
    """
  
  val p = ParamHandler(new Date())
  println(ParamHandler.extractParams(str))
}
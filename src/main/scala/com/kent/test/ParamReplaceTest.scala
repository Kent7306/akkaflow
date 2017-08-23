package com.kent.test

import com.kent.coordinate.ParamHandler

object ParamReplaceTest extends App{
  val str = """
    <work-flow name="wf_test" dir="/example" desc="导入item数据">

    	<start name="start" to="watch_log" />
    
    	<action name="data_monitor" retry-times="2" interval="8" timeout="500" host="127.0.0.1" desc = "监测日志文件">
    		<data-monitor category="database" source-name="t_item" is-saved="true" is-exceed-error="true" time-mark="${param:stadate}">
    		    <source type="MYSQL" jdbc-url="jdbc:mysql://localhost:3306/wf?useSSL=false" username="root" password="root">
    		        <code>
    		        select count(1) from workflow
    		        </code>
    		    </source>
    		    <min-threshold type="NUM">
    		        <code>0</code>
    		    </min-threshold>
    		    <max-threshold type="NUM">
    		        <code>123434</code>
    		    </max-threshold>
    		    <warn-msg>xxxxx,若无配置，则自动生成告警消息</warn-msg>
    		</data-monitor>
    		<ok to="watch_log"/>
    	</action>
    	<action name="watch_log" retry-times="2" interval="8" timeout="500" host="127.0.0.1" desc = "监测日志文件">
    		<file-executor>
    			<command>/tmp/tmp/2017-08-07/test.sh</command>
    			<attach-list>
    				<file>/tmp/tmp/2017-08-07/1111</file>
    				<file>/tmp/tmp/2017-08-07/2222</file>
    			</attach-list>
    		</file-executor>
    	    <ok to="end"/>
    	</action>
    	
    	<end name="end"/>
    </work-flow>
    """
  
}
## 工作流定义文档说明 
很多时候，在数据处理过程当中，或数据仓库构建过程当中，产生一系列子任务按一定顺序组织，类似有向无环拓扑图。一个处理过程，从开始节点，多个子任务并发或串行执行，由结束节点终止该过程，这样的一个处理过程，可以称作`工作流`，`akkaflow`框架的工作流的定义参考了`Oozie`的语法，但相对于`Oozie`更为轻量级和简便使用。</br>


#####一个工作流文件定义示例
```xml
<work-flow name="wf_join_1" mail-level = "W_SUCCESSED,W_FAILED,W_KILLED" mail-receivers="15018735011@163.com,492005267@qq.com"
dir="/tmp/test" instance-limit="4">
     <start name="start_node_1" to="fork_node" />
     <fork name="fork_node">
         <path to="action_node_1" />
         <path to="action_node_2" />
     </fork> 
     <action name="action_node_1" retry-times="3" interval="10" timeout="500" host="127.0.0.1" desc = "删除某文件">
         <shell>
             <command>cd /tmp; rm -f xxxx</command>
         </shell>
         <ok to="join_node"/>
         <error to="kill_node"/>
     </action>
     <action name="action_node_2" retry-times="1" interval="1" timeout="500" desc = "远程执行脚本">
       <script>
           <content><![CDATA[
            cd /home/kent/tmp
            echo "begin sleep 3 s"
            sleep 3
            echo "sleep finished"
            ]]></content>
       </script>
       <ok to="join_node"/>
       <error to="kill_node"/>
     </action>
     <kill name="kill_node">
         <message>某个行动节点执行失败</message>
     </kill>
     <join name="join_node" to="end_node"/>
     <end name="end_node"/>
 </work-flow>
```
</br>
### &lt;workflow/&gt;
整个工作流的节点定义在`workflow`标签中,若工作流定义在一个文件中，这`workflow`作为最外层的标签。</br>
####* 属性
`name`：必填，工作流名称，用来标识唯一的工作流</br>
`mail-level`：可选，无。工作流邮件级别，当工作流实例执行完毕后，根据执行后状态来决定是否发送邮件，目前工作流实例执行完毕后有三种状态（杀死，失败，成功），对应值为`W_SUCCESSED`, `W_FAILED`, `W_KILLED`。</br>
`mail-receivers`： 可选，默认无。工作流邮件接受者，指定哪些邮箱可接收该工作流执行情况反馈</br>
`dir`：可选，该工作流的存放目录，默认为/tmp。</br>
`instance-limit`:可选，该工作流同时运行的实例上限，默认不设上限</br>
#### * 标签内容
`workflow`标签中包含各定义的节点，节点类型分为两大类，控制节点与动作节点。</br>
####* 示例
```xml
<!-- example all paramter -->
<workflow name="wf_join_1" 
  mail-level = "W_SUCCESSED,W_FAILED,W_KILLED" 
  mail-receivers="15018735011@163.com,492005267@qq.com"
  desc="这是一个测试工作流"
  dir="/tmp">
...
</workflow>

<!-- example mini -->
<workflow name="wf_join_2">
	....
</workflow>
```
</br>

###控制节点标签

#### &lt;start/&gt;
开始节点，工作流开始的地方，每个工作流只能有一个`start`节点。</br>
#####* 属性
`name`：必填，该节点名称，唯一标识</br>
`to`：必填，指定下一个执行节点</br>
#####* 示例
```xml
<!-- example 1 -->
<start name="start_node" to="next_node"／>
```

####&lt;fork/&gt;
分发节点，分发出多个节点任务并行执行</br>
#####* 属性
`name`：必填，节点名称，唯一标识</br>
#####* 标签内容
`<path/>`： 子标签, 指定多个并行执行的节点</br>
`path`的属性`to`： 必填，指定下一个执行节点</br>
#####* 示例
```xml
<!-- example 1 -->
<fork name="fork_node">
	<path to="next_node_1"/>
	<path to="next_node_2"/>
	<path to="next_node_3"/>
</fork>
```

####&lt;kill/&gt;
kill节点，用来杀死当前工作流实例，而被杀死的工作流实例状态为`W_KILLED`。</br>
#####* 属性
`name`：必填，该节点名称，唯一标识</br>
#####* 标签内容：
`<message/>`，子标签，标签中内容为节点杀死附带信息。</br>
#####* 示例
```xml
<!-- example 1 -->
<kill name="kill_node">
	<message>该工作流实例被杀死</message>
</kill>
```

####&lt;join/&gt; 
把多个分支节点合并为一个分支节点,各个节点中的下一个节点指向join节点，都会被合并，当所有执行`join`的节点全部完成时，才开始往下执行。</br>
#####* 属性
`name`： 必填，节点名称，唯一标识</br>
`to`： 必填，指定下一个执行节点，当所有分支节点完成后，才会开始执行`to`指定的节点</br>
#####* 示例
```xml
<!-- example 1 -->
<join name="join_node" to="next_node"/>
```
####&lt;end/&gt;
结束节点，工作流结束的地方，每个工作流中必须要有一个`end`节点，并且工作流实例执行`end`节点后，该工作流实例执行完成，状态标识为`W_SUCCESSED`。</br>
#####* 属性
`name`： 必填，节点名称，唯一标识
#####* 示例
```xml
<!-- example 1 -->
<end name="end_node"/>
```
</br>
###行动节点

####&lt;action/&gt; 
执行特定任务的节点，可以分发到不同的服务器上执行，具有容错并重试机制。
#####* 属性
`name`： 必填，节点名称，唯一标识</br>
`host`：可选，指定该动作节点在某台机器运行，默认为空，随机选定一个节点执行。</br>
`retry-times`：可选，默认为0，节点执行失败后重试次数</br>
`interval`：可选，默认为0，节点执行失败后等待重新执行的时间间隔（秒）</br>
`timeout`：可选，默认为-1，即不会超时，`timeout`是整个节点生命周期的超时限定，包括重试执行的时间，单位秒。</br>
`desc`：可选，默认无，节点描述</br>
#####* 标签内容: 
`<ok/>`，子标签，属性`to`，节点执行成功指向下一节点</br>
`<error/>`，子标签，属性`to`，节点重试后仍然失败时，指向下一节点</br>
可以指定不同类型的动作子标签，目前有`<command/>`, `<script/>`, `<file-watcher/>`, 动作子标签类型说明详见下文</br>
</br>
#####* 示例
```xml
<!-- example 1 -->
<action name="node_1" host="127.0.0.1" retry-time=10 interval=300 timeout=6000 desc="action example desc">
	...
	<ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>

<!-- example 2 -->
<action name="node_2">
	...
	<ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>
```

###行动子标签类型
具有不同行为的行动节点

####&lt;shell/&gt;
远程shell命令执行，可以执行指定所部署机器的脚本文件，也可以执行某些脚本命令</br>
#####* 标签内容
`<command/>`： 子标签，填写执行命令</br>
#####* 示例
```xml
<!--example 1 -->
<action name="action_node_1" desc="执行hdfs命令，删除某集群目录">
    <shell>
        <command>hdfs dfs -rm -r /xxx/xxx/xxxx</command>
    </shell>
    <ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>
<!-- example 2 -->
<action name="action_node_1" host="127.0.0.1" desc="执行指定服务器上的脚本文件">
    <shell>
        <command>/home/tmp/run_data.sh ${time.yestoday|yyyy-MM-dd}</command>
    </shell>
    <ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>
```
####&lt;script/&gt;
#####* 标签内容
远程脚本代码执行的行动节点，
`<content>`：子标签,该标签内容存放执行的脚本代码</br>
`<location>`：子标签，可选，该标签内容生成的文件路径，默认文件会放在`application.conf`中参数指定的目录中，并且文件名为八位的UID</br>
#####* 示例
```xml
<!-- example 1 -->
<action name="action_node_1" host="127.0.0.1" desc="远程执行某指定脚本">
	<script>
	   <content><![CDATA[
	    #!/bin/perl
	    print "hello\n";
	    print "end\n";
	    ]]></content>
	</script>
	<ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>
<!-- example 2 -->
<action name="action_node_1" host="127.0.0.1" desc="远程执行某指定脚本">
	<script>
	   <location>/tmp/akkaflow/test.sh</location>
	   <content><![CDATA[
	    #!/bin/bash
	    cd /home/kent/tmp
	    echo "begin sleep 3"
	    sleep 3
	    echo "sleep finished"
	    ]]></content>
	</script>
	<ok to="next_ok_node"/>
	<error to="next_error_node"/>
</action>
```
####&lt;file-watch/&gt;
#####* 标签内容
`<file/>`：子标签，必填，该子标签的属性`dir`，必填，监控目录，文件系统类型可为`本地文件系统`，`hdfs`，`ftp`，当前只支持本地文件系统，其他两种后续添加，属性`num-threshold`，可选，文件个数阈值，默认为1，要高于阈值才通过</br>
`<size-warn-message/>`，可选，检测监控的文件大小是否高于指定阈值，若低于，则邮件告警。该子标签属性`enable`，可选，是否可用，默认为不可用；属性`size-threshold`，必填，文件大小阈值，可用1GB，2.3M，1.1kb直观的写法，若存在文件大小低于阈值，则告警处理</br>
`size-warn-message`子标签内容：告警邮件内容，若无配置（留空），则告警邮件内容由系统生成（更为详细）</br>
#####* 示例
```xml
<!-- example 1 -->
<action name="action_node_1" host="127.0.0.1" desc="监控本地文件">
	<file-watcher>
	    <file dir="/Users/kent/Documents/tmp" num-threshold="1">*.sh</file>
	    <size-warn-message enable="true" size-threshold="1532MB">
	    <![CDATA[
	      文件容量小于1532M，请联系xxx进行确认
	    ]]>
	    </size-warn-message>
	    <ok to="next_ok_node"/>
		<error to="next_error_node"/>
	</file-watcher>
</action>

<!-- example 2 -->
<action name="action_node_1" desc="监控hdfs">
	<file-watcher>
	    <file dir="hdfs:///user/kent/log/${time.yestoday|yyyy/MM/dd}" num-threshold="1">*.sh</file>
	    <size-warn-message enable="true" size-threshold="2MB"/>
	    <ok to="next_ok_node"/>
		<error to="next_error_node"/>
	</file-watcher>
</action>
```


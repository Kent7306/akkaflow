## 工作流定义文档说明
很多时候，在数据处理过程当中，或数据仓库构建过程当中，产生一系列子任务按一定顺序组织，类似有向无环拓扑图。一个处理过程，从开始节点，多个子任务并发或串行执行，终止于结束节点，这样的一个处理过程，可以称作`工作流`。 
##### 一个工作流文件定义示例
```xml
<work-flow name="test_a" creator="Kent" mail-receivers="492005267@qq.com"
    dir="/TMP" desc="测试a">
  <!-- 调度配置 -->
  <coordinator is-enabled="true">
      <depend-list cron="0 */2 * * *"></depend-list>
      <param-list>
          <param name="stadate" value="${time.today|yyyy-MM-dd|-1 day}"/>
      </param-list>
  </coordinator>
  <!-- 节点列表 -->
  <start name="start" to="data_monitor" />

  <action name="data_monitor" retry-times="5" interval="1" desc = "监测源数据">
      <data-monitor>
          <source type="SQL" db-link="local_oracle">
              select count(1) from dual
          </source>
          <min-threshold type="NUM">1</min-threshold>
          <warn-msg>填写检测异常信息</warn-msg>
      </data-monitor>
      <ok to="script"/>
  </action>

  <action name="script" desc = "执行脚本">
      <script>
          <code><![CDATA[
            for i in `seq 0 20`;do
                echo -e "$i****${param:stadate}***";
            done
          ]]></code>
      </script>
      <ok to="end"></ok>
  </action>

  <end name="end"/>
</work-flow>
```  

### &lt;workflow/&gt;
整个工作流的节点定义在`workflow`标签中,若工作流定义在一个文件中，这`workflow`作为最外层的标签.  
#### * 属性
* `name`：必填，工作流名称，用来标识唯一的工作流
* `creator`：可选，默认为Unknown，当前工作流创建者
* `mail-level`： 可选，默认为失败。工作流邮件级别，当工作流实例执行完毕后，根据执行后状态来决定是否发送邮件，目前工作流实例执行完毕后有三种状态（杀死，失败，成功），对应值为`杀死`, `失败`, `成功`。
* `mail-receivers`： 可选，默认无。工作流邮件接受者，指定哪些邮箱可接收该工作流执行情况反馈
* `dir`：可选，该工作流的存放目录，默认为/tmp。
* `instance-limit`: 可选，该工作流同时运行的实例上限，默认不设上限

#### * 子标签
* `workflow`标签中包含各定义的节点，节点类型分为三大类，调度配置节点、控制节点、动作节点。

#### * 示例
```xml
<!-- example all paramter -->
<workflow name="wf_join_1"  mail-level = "成功,失败,杀死" mail-receivers="15018735011@163.com,492005267@qq.com" desc="这是一个测试工作流" dir="/tmp">
    ...
</workflow>

<!-- example mini -->
<workflow name="wf_join_2">
	....
</workflow>
```
### 调度器配置	&lt;coordinator/&gt;
可配置时间触发、前置依赖或者二者结合，其中时间触发类似于Linux上的crontab功能，指定某个时间点触发当前工作流；而前置依赖是依赖于某个工作流的完成来触发。akkflow的coordinator既可以配置时间与依赖触发，也可以二者组合配置。另外，调度器中可配置参数，并且有内置参数与相关系统参数。
#### * 属性
`start-time`：可选，调度器的开始生效时间，默认从很久以前开始，格式为，yyyy-MM-dd HH:mm:ss</br>
`end-time`：可选，调度器的开始失效时间，默认从很久以后结束，格式为，yyyy-MM-dd HH:mm:ss</br>
`is-enabled`：可选，是否生效，默认为true。</br>

#### * 子标签
##### &lt;depend-list/&gt;
只有满足时间触发，并且前置依赖任务成功执行完成，才能触发该调度器执行后置依赖工作流。</br>
`cron`属性，可选，默认无，时间触发设置，类似于linux中的crontab周期时间点配置。</br>
`<workflow>`子标签，必填，前置依赖的任务流，可配置多个，`name`属性，必填，指定依赖的任务流</br>
示例</br>
```xml
<!-- 每天1点触发，并且前置依赖于wf_1，wf_2 -->
<depend-list cron="0 1 * * *">
        <workflow name="wf_1" />
        <workflow name="wf_2" />
</depend-list>
<!-- 每周一午夜触发 -->
<depend-list cron="midnight on monday"></depend-list>
```

##### &lt;param-list/&gt;
参数列表，转化后的参数列表作为触发后的后置工作流实例的参数，在工作流定义中看到`${param:xxx}`，就是使用了相关参数。参数包括自定义参数与内置参数，目前内置参数只包含时间相关操作的参数，注意参数顺序，下面的参数可以使用上面的参数。</br>
`<param/>`子标签，可选，自定义参数，属性`name`为参数变量名，属性`value`为参数值，`value`可以引用其他自定义变量或内置变量，多个子标签`<param/>`时，相同参数变量，下面的会替换上面的参数；下面的参数值可以引用上面的参数变量。</br>

###### 内置时间变量
`time.today` -> 当前日期，格式为yyyy-MM-dd，如2017-01-07</br>
`time.yestoday` -> 昨天日期，格式为yyyy-MM-dd，如2017-01-06</br>
`time.cur_month` -> 当前月份，格式为yyyy-MM，如2017-01</br>
`time.last_month` -> 上个月份，格式为yyyy-MM，如2016-12</br>
时间格式化和计算
`${time.today|yyyy-MM-dd|-1 day}` -> 假如当前日期为2017-01-07，那么结果为2017-01-06；当前日期减少一天，然后格式化。</br>
`${time.last_month|yyyy-MM|2 month}` -> 假如当前日期为2017-01-07，那么结果为2017-02；当前月份加两个月，然后格式化。
示例</br>
`${time.today|yyyy-MM-dd HH:mm:ss|-1 hour}` -> 假如当前时间为2017-01-07 12:12:12，那么结果为2017-01-07 11:12:12；当前时间减去一个小时，然后格式化。</br>
目前支持计算的时间单位有`month`, `day`, `hour`, `minute`</br>
示例</br>
```xml
<param-list>
	<!--定义参数action，取值delete-->
   <param name="action" value="delete"/>
	<!--定义参数yestory，取值用内置时间变量计算-->
   <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
   <!--定义参数yestoday2，取值用内置时间变量计算-->
   <param name="yestoday2" value="${time.yestoday|yyyy/MM/dd}"/>
   <!--定义参数hdfs_dir，取值引用上面的参数yestoday2-->
   <param name="hdfs_dir" value="hdfs:///user/kent/log/${yestoday2}/*"/>
</param-list>
```
### 控制节点标签

#### &lt;start/&gt;
开始节点，工作流开始的地方，每个工作流只能有一个`start`节点。
##### * 属性
* `name`：必填，该节点名称，唯一标识
* `to`：必填，指定下一个执行节点

##### * 示例
```xml
<!-- example 1 -->
<start name="start_node" to="next_node"／>
```

#### &lt;fork/&gt;
分发节点，分发出多个节点任务并行执行
##### * 属性
* `name`：必填，节点名称，唯一标识

##### * 子标签
* `<path/>`： 子标签, 指定多个并行执行的节点，属性`to`： 必填，指定下一个执行节点

##### * 示例
```xml
<!-- example 1 -->
<fork name="fork_node">
	<path to="next_node_1"/>
	<path to="next_node_2"/>
	<path to="next_node_3"/>
</fork>
```

#### &lt;kill/&gt;
kill节点，用来杀死当前工作流实例，而被杀死的工作流实例状态为`杀死`。
##### * 属性
* `name`：必填，该节点名称，唯一标识

##### * 子标签：
* `<message/>`，子标签，可选，标签中内容为节点杀死附带信息，发送告警邮件时会附带改信息

##### * 示例
```xml
<!-- example 1 -->
<kill name="kill_node">
	<message>该工作流实例被杀死</message>
</kill>
```

#### &lt;join/&gt; 
把多个分支节点合并为一个分支节点,各个节点中的下一个节点指向join节点，都会被合并，当所有执行`join`的节点全部完成时，才开始往下执行。  
##### * 属性
`name`： 必填，节点名称，唯一标识
`to`： 必填，指定下一个执行节点，当所有分支节点完成后，才会开始执行`to`指定的节点

##### * 示例
```xml
<!-- example 1 -->
<join name="join_node" to="next_node"/>
```
#### &lt;end/&gt;
结束节点，工作流结束的地方，每个工作流中必须要有一个`end`节点，并且工作流实例执行`end`节点后，该工作流实例执行完成，状态标识为`W_SUCCESSED`。
##### * 属性
* `name`： 必填，节点名称，唯一标识

##### * 示例
```xml
<!-- example 1 -->
<end name="end_node"/>
```
  
### 行动节点

#### &lt;action/&gt; 
执行特定任务的节点，可以分发到不同的服务器上执行，具有容错并重试机制。
##### * 属性
* `name`： 必填，节点名称，当前工作流范围内唯一标识
* `host`：可选，指定该动作节点在某台机器运行，默认为空，随机选定一个节点执行。
* `retry-times`：可选，默认为0，节点执行失败后重试次数
* `interval`：可选，默认为0，节点执行失败后等待重新执行的时间间隔（秒）
* `timeout`：可选，默认为-1，即不会超时，`timeout`是整个节点生命周期的超时限定，包括重试执行的时间，单位秒。
* `desc`：可选，默认无，节点描述

##### * 子标签: 
* `<ok/>`，子标签，属性`to`，节点执行成功指向下一节点
* `<error/>`，子标签，属性`to`，节点重试后仍然失败时，指向下一节点
* 可以指定不同类型的动作子标签，目前有 `<script/>`, `<file-monitor/>`，`<sql/>`,`<shell/>`,`<transfer/>`,`<data-monitor/>`,`<file-executor/>`, 动作节点标签类型说明详见下文


##### * 示例
```xml
<!-- example 1 -->
<action name="node_1" host="127.0.0.1" retry-times="10" interval="300" timeout="6000" desc="action example desc">
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

###行动节点标签类型
具有不同行为的行动节点
#### &lt;script/&gt;
脚本执行节点，在指定host配置的目录下，生成该脚本文件并执行，脚本执行当前命令为该目录；并且会把相关配置的附件文件也拷贝到该目录中。
##### * 子标签
* `<code>`：子标签,该标签内容存放执行的脚本代码
* `<attach-list>`：子标签，可选，附件列表，需要吧本机的某个文件拷贝到某个worker节点上。

##### * 示例
```xml
<!-- example 1 以perl方式来清洗日志-->
<script>
   <code>
   <![CDATA[
   #!/usr/bin/perl
   $stime="${param:stime}";
   $file="./order.log";
   $result="/tmp/order.result";
   open(FH,'<',$file) or die("no such file");
   open(FR,'>',$result);
   while(<FH>){
   chomp $_;
   @cols = split(/-/,$_);
   $str = join(",",@cols);
   print FR $stime.",".$str."\n";
   }
   close FH;
   close FR;
   ]]>
  </code>
  <attach-list>
       <file>example/import_order/order.log</file>
   </attach-list>
</script>
<!-- example 2 远程执行某指定脚本-->
<script>
   <code><![CDATA[
    #!/bin/bash
    echo "begin sleep 3"
    sleep 3
    echo "sleep finished"
    ]]></code>
</script>
```
#### &lt;sql/&gt;
sql执行节点，支持多种数据库sql执行，目前支持的数据库有，Hive，Mysql，Oracle，Impala，基于jdbc来执行sql，sql执行出错就会执行失败。
##### * 属性
* `db-link`，属性，必填，数据库连接串，需要提前配置，文件文件放于`config/db_links.xml`。

##### * 子标签
* 无，直接写sql，多个sql用分号；来分割。

##### * 示例
```xml
<!-- example 1-->
<sql db-link="local_orcl">
 	delete from test_aa where 2 > 3
</sql>
<!-- example 2-->
<sql db-link="local_mysql">
   create table aaa (col varchar(10));
 	insert into aaa values ('111');
 	delete from aaa where 1 = 1;
 	drop table aaa; 
</sql>
```

#### &lt;file-monitor/&gt;
文件监控节点，监控某个文件系统中的某目录下的特定文件是否符合要求，包括文件数量与文件大小，若超出阈值，则可邮件告警，并且节点执行失败。
##### * 子标签
* `<file>`：必填，文件系统类型可为`本地文件系统`，`hdfs`，`ftp`，当前只支持本地文件系统，属性`num-threshold`，可选，文件个数阈值，默认为1，要高于阈值才通过;属性`size-threshold`，必填，文件大小阈值，可用1GB，2.3M，1.1kb直观的写法，若存在文件大小低于阈值，则告警处理
* `<warn-msg>`：`标签内容`可选，告警邮件补充内容。

##### * 示例
```xml
<!-- example 1 监控本地文件-->
<file-monitor>
    <file num-threshold="1" size-threshold="2MB">/home/you/app/dir/*.sh</file>
    <warn-msg>请填写异常告警信息</warn-msg>
</file-monitor>

<!-- example 2 监控hdfs-->
<file-monitor>
    <file num-threshold="10" size-threshold="2G">hdfs:///home/you/app/dir/*.sh</file>
    <warn-msg>请填写异常告警信息</warn-msg>
</file-monitor>
```

#### &lt;data-monitor/&gt;
数据监控节点，可以监控数据库的记录，文件行数，文件大小等不同数据类型的指标，并设置该数据点的上下限。
##### * 属性
* `category`，属性，必填，数据源分类，任意有意义字符串即可
* `source-name`，属性，必填，数据源名称，任意有意义字符串即可
* `time-mark`，属性，必填，时间标志点，可填入 YYYY-MM-DD或YYYY-MM-DD hh: mm: ss等格式的时间点
* `is-saved`，属性，可选，默认为false，是否对监控数据进行保存，当为true时，要设置`category`，`source-name`与`time-mark`属性。
* ` is-exceed-error`，属性，可选，默认为false，是否超过阈值就设置为执行失败，若为true，则会执行成功。

##### * 子标签
* `<source>`，子标签，必填，监控数据源，`type`属性，指定数据源类型，当前可选项为`SQL`,`COMMAND`,`NUM`,其中，当选择为SQL数据源时，要补充填写`db-link`属性配置；当选择`COMMAND`与`NUM`时，则不需要；标签内容，当选择SQL时，标签内容填写SQL，查询结果只有单值；当选择`COMMAND`时，则可填写shell命令，返回也只能是单数据值；当选择`NUM`	时，可直接填写单数据值。
*  `<min-threshold>`：子标签，可选，最小阈值配置，配置项与上面`source`一样
*  `<max-threshold>`：子标签，可选，最大阈值配置，配置项与上面`source`一样

##### * 示例
```xml
<!-- example 1 监测数据库记录-->
<data-monitor>
     <source type="COMMAND">ORACLE -e "select count(1) from dual"</source>
     <min-threshold type="NUM">1</min-threshold>
     <warn-msg>填写检测异常信息</warn-msg>
</data-monitor>

<!-- example 2 监测日志文件-->
<data-monitor>
   <source type="SQL" db-link="local_mysql">
       select count(1) from example_item where ds = '${param:stime}'
   </source>
   <min-threshold type="NUM">2</min-threshold>
   <max-threshold type="SQL" db-link="local_mysql">
       select count(1)+10 from example_item where ds = '${param:stime}'
   </max-threshold>
</data-monitor>
```
#### &lt;transfer/&gt;
数据传输节点，支持通用jdbc连接的数据库,本地文件系统，HDFS之间的数据记录互传，hive相关的数据传输采用sqoop，但是并没有进一步封装（后续有时间调整），所以只是提供了写脚本命令的方式（需要指定到有按照sqoop的机器上执行）
##### * 子标签
* `<source>`：可选，导入的数据源配置，与下面的`<script/>`标签二选一，数据来源支持jdbc数据源与各类型文件。属性`type` ,可选值有`DB`,`FILE`,其中FILE是各类型文件。在FILE类型下，属性`delimited`是文件分隔符,标签内容填写文件路径；在DB情况下，需填进属性`db-link`,标签内容填写表名或查询sql。
* `<target>`: 可选，导出的目标源配置，与source标签一起使用，属性`type`，可选值有`DB`,`FILE`。在FILE类型下，属性`path`与`delimited`默认是本地文件路径与文件分隔符,属性`is-pre-del`,可选，是否在导入数据前删除文件，默认为false；在DB情况下，需填进属性`db-link`，属性`is-pre-del`,可选，是否在导入数据前清空表，默认为false,标签`<pre>`与`<after>`，前置执行的sql与后置执行的sql，若无，可不添加。
* `<script>`: 由于sqoop还没封装，所以提供以脚本命令的方式来进行数据传输。

#### * 示例
```xml
<!-- source jdbc 方式-->
<source type="DB" db-link="local_mysql">order_item</source>
<!-- source 读取本地文件 其中delimited 默认是以tab来作为分隔符 -->
<source type="FILE" delimited="\t">/home/kent/info.txt</source>
<!-- target 目标jdbc数据源 -->
<target type="DB" db-link="local_mysql" table="bbb">
    	<pre>create table if not exists bbb like log_record</pre>
</target>
<!-- target 目标写入本地文件 -->
<target type="FILE" delimited="\t" is-pre-del="false" path="/home/kent/info.txt">
    	<pre>rm -f xxx</pre>
    	<after>xxxx</after>
 </target>
 <!-- 用脚本方式使用sqoop导入导出-->
<script>sqoop import xx xxx xx</script>

<!-- 完整示例1 -->
<transfer> 
	    <source type="FILE" delimited=",">hdfs://quickstart.cloudera:8022/tmp/1111.txt</source>
	    <target type="FILE" path="/tmp/2222.txt">
	    	  <pre>echo "begin"</pre>
	    	  <after>echo "end"</after>
	    </target>
	</transfer>

<!-- 完整示例2 -->
<transfer>
	<script>
	sqoop import xxx xxx xx
	</script> 
</transfer>
```
#### &lt;file-executor/&gt;
脚本文件分发执行节点，把当前活动的master机器上执行的脚本文件以及其他附件分发到Worker上执行。
##### * 子标签
* `<command>`，必填，执行某个脚本运行
*  `<attach-file>`：可选，附件列表，需要吧本机的某个文件拷贝到某个worker节点上。

##### * 示例
```xml
<!-- example 1 清洗日志并导入数据库-->
<file-executor>
   <command>sh /xx/xx/import_item/clean_import.sh "${param:stime}"</command>
   <attach-list>
       <file>example/import_item/item_1.log</file>
       <file>example/import_item/item_2.log</file>
   </attach-list>
</file-executor>

<!-- example 2 -->
<file-executor>
   <command>perl /xx/xx/import_item/clean_import.pl "${param:stime}"</command>
</file-executor>
```


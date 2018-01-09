## 调度器定义文档说明
`akkaflow`的调度器称作`coordinator`，可配置时间触发与依赖触发，其中时间触发类似于Linux上的crontab功能，指定某个时间点触发某工作流`workflow`；而依赖触发即依赖于某个工作流的完成触发下一个工作流。`akkflow`的`coordinator`既可以配置时间与依赖触发，也可以二者组合配置。另外，调度器中可配置参数，并且有内置参数与相关系统参数，提供给触发的工作流使用。

#####一个调度器文件定义示例
```xml
<coordinator name="coor_join_order_and_item" is-enabled="false" dir="/example" desc="依赖启动">    
    <depend-list cron="*/30 * * * *">
        <workflow name="wf_import_item" />
        <workflow name="wf_import_order" />
    </depend-list>
    <trigger-list>
      <workflow name="wf_join_order_and_item"></workflow>
    </trigger-list>
    <param-list>
        <param name="stime" value="${time.today|yyyy-MM-dd hh:mm}"/>
        <param name="yestory" value="${time.yestoday|yyyy-MM-dd}"/>
        <param name="yestoday2" value="${time.today|yyyyMMdd|-1 day}"/>
        <param name="stadate" value="${time.today|yyyy-MM-dd hh}"/>
    </param-list>
</coordinator>
```
###&lt;coordinator/&gt;
整个调度器配置定义在`coordinator`标签中,`coordinator`作为最外层的标签。
####* 属性
`name`：必填，名称，用来标识唯一的`coordinator`.</br>
`start-time`：可选，调度器的开始生效时间，默认从很久以前开始，格式为，yyyy-MM-dd HH:mm:ss</br>
`end-time`：可选，调度器的开始失效时间，默认从很久以后结束，格式为，yyyy-MM-dd HH:mm:ss</br>
`creator`：可选，创建者，默认为Unknown。</br>
`is-enabled`：可选，是否生效，默认为true。</br>
`desc`：可选，调度器描述。</br>
`dir`：可选，该coordinator的存放目录，默认为/tmp。</br>
####* 示例
```xml
<!-- example 1 full-->
<coordinator name="coor1_2" creator="kent" start-time="2016-09-10 10:00:00" end-time="2017-09-10 10:00:00" desc="this is a coordinator" dir="/tmp/test"> 
	...
</coordinator>

<!-- example 2 simplified-->
<coordinator name="coor1_3">
	...
</coordinator>
```
####* 标签内容
####&lt;depend-list/&gt;
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
<!-- 前置依赖于wf_1，wf_2 -->
<depend-list>
        <workflow name="wf_1" />
        <workflow name="wf_2" />
</depend-list>
```
####&lt;trigger-list/&gt;
配置的后置触发工作流列表，当前置依赖被满足后，会触发后置任务启动。</br>
`<workflow/>`子标签，必填，可以包含多个子标签，属性`name`选择后置触发的工作流名称。</br>
示例</br>
```xml
<trigger-list>
      <workflow name="wf_3"></workflow>
      <workflow name="wf_4"></workflow>
</trigger-list>
```
####&lt;param-list/&gt;
参数列表，转化后的参数列表作为触发后的后置工作流实例的参数，在工作流定义中看到`${param:xxx}`，就是使用了相关参数。参数包括自定义参数与内置参数，目前内置参数只包含时间相关操作的参数，注意参数顺序，下面的参数可以使用上面的参数。</br>
`<param/>`子标签，可选，自定义参数，属性`name`为参数变量名，属性`value`为参数值，`value`可以引用其他自定义变量或内置变量，多个子标签`<param/>`时，相同参数变量，下面的会替换上面的参数；下面的参数值可以引用上面的参数变量。</br>

#####内置时间变量
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


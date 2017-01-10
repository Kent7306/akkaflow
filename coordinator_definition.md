## 调度器定义文档说明
`akkaflow`的调度器称作`coordinator`，可配置时间触发与依赖触发，其中时间触发类似于Linux上的crontab功能，指定某个时间点触发某工作流`workflow`；而依赖触发即依赖于某个工作流的完成触发下一个工作流。`akkflow`的`coordinator`既可以配置时间与依赖触发，也可以二者组合配置。另外，调度器中可配置参数，并且有内置参数与相关系统参数，提供给触发的工作流使用。

#####一个调度器文件定义示例
```xml
<coordinator name="coor1_2" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00" id="0002">    
    <trigger>
        <cron config="* * * * * *"/>
        <depend-list>
          <depend wf="wf_1" />
          <depend wf="wf_2" />
        </depend-list>
    </trigger>
    <workflow-list>
      <workflow path="wf_3"></workflow>
      <workflow path="wf_4"></workflow>
    </workflow-list>
    <param-list>
        <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
        <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
        <param name="yestoday2" value="${time.yestoday|yyyy/MM/dd}"/>
        <param name="hdfs_dir" value="hdfs:///user/kent/log/${yestoday2}/*"/>
    </param-list>
</coordinator>
```
###&lt;coordinator/&gt;
整个调度器配置定义在`coordinator`标签中,`coordinator`作为最外层的标签。
####* 属性
`name`：必填，名称，用来标识唯一的工作流

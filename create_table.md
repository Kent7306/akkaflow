
### MYSQL建表语句
`akkaflow`架构是可以不需要数据持久化软件来持久化数据的，运行时，相关的工作流信息、调度器信息等数据会存放于内存中，而工作流执行过程会通过控制台打印，但一旦程序停止，就会丢失；所以提供了通过`MYSQL`保存相关数据，并且通过数据存放于`MYSQL`，可以跟踪工作流实例的执行过程，也可以从总体上参看调度情况，当然，这取决于自己开发的前台展示界面，而`akkaflow`尽可能保留相关的执行数据。</br>
#### 设置编码格式及创建数据库
```sql
set character_set_server = utf8;
set character_set_database = utf8;
create database wf;
```

#### workflow信息表
保存工作流信息</br>
```sql 
use wf;
drop table workflow;
create table if not exists workflow(
    name varchar(128) primary key  not null,
    creator varchar(128),
    dir varchar(128),
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    instance_limit int,
    params JSON comment '参数名称集合,例: [sdate,otype]',
    xml_str text comment 'xml的内容',
    create_time datetime,
    last_update_time datetime
);
```

#### coordinator配置表
保存调度器信息</br>
```sql
drop table coordinator;
create table if not exists coordinator (
    name varchar(128) primary key,
    creator varchar(128),
    param JSON,
    dir varchar(128),
    cron varchar(128),
    depends JSON comment '工作流依赖id集合，例：[{name:xxxx, status:0},{name:xxx,status:0}]',
    triggers JSON comment '触发工作流name集合，例：[xxxx，xxxx]',
    stime datetime,
    etime datetime,
    is_enabled int,
    description varchar(1024),
    xml_str text comment 'xml的内容', 
    create_time datetime,
    last_update_time datetime
);
```

#### node配置表
保存工作流节点信息</br>
```sql
drop table node;
create table if not exists node (
    name varchar(128) not null,
    is_action int(1) comment '是否为action节点',
    type varchar(100) comment '节点类型',
    content JSON comment '节点存放内容',
    workflow_name varchar(128) comment '外键->workflow_info:name',
    description varchar(1024)
);    
```

#### workflow实例表
保存工作流实例执行信息，整个工作流实例执行过程都会同步更新到该表中</br>
```sql
drop table workflow_instance;
create table if not exists workflow_instance(
    id varchar(8) primary key not null,
    name varchar(128) not null,
    creator varchar(128),
    dir varchar(128),
    param JSON,
    status int(1) comment "0:就绪，1:运行中，2:挂起，3:执行成功，4:执行失败，5:杀死",
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    instance_limit int,
    stime datetime,
    etime datetime,
    create_time datetime,
    last_update_time datetime,
    xml_str text comment 'xml的内容'
);
```

#### coordinator触发记录表
记录调度器的触发情况，不过日志表中也有相关记录，该表暂时保留，后续会丰富信息。</br>
```sql
drop table coordinator_trigger_record;
create table if not exists coordinator_trigger_record (
    id varchar(8) primary key comment '标识',
    coordinator_name varchar(128),
    workflow_name varchar(128),
    param JSON,
    trigger_time datetime
);
```

#### node实例表
保存工作流实例中每个节点的执行情况，整个工作流实例执行过程都会同步更新到该表中</br>
```sql
drop table node_instance;
create table if not exists node_instance (
    workflow_instance_id varchar(8) not null,
    name varchar(200) not null,
    is_action int(1) comment '是否为action节点',
    type varchar(100) comment '节点类型',
    content JSON comment '节点存放内容',
    description varchar(1024),
    status int(1) comment "0:就绪，1:运行中，2:挂起，3:执行成功，4:执行失败，5:杀死",
    stime datetime,
    etime datetime,
    msg varchar(1024),
    index(workflow_instance_id)
);
```

#### 日志表
各个阶段的执行日志会保存在该表当中。</br>
其中，`sid`字段可能为工作流实例id，节点id，这取决于`ctype`字段，`ctype`为不同的对象类型;`level`字段为日志级别。</br>
```sql
drop table log_record;
create table if not exists log_record (
    id int(10) primary key auto_increment,
    level varchar(10),
    stime datetime(3),
    ctype varchar(60),
    sid varchar(20),
    name varchar(60),
    content varchar(1024)
);
```

#### 各个阶段的执行日志会保存在该表当中。</br>
其中，`sid`字段可能为工作流实例id，节点id，这取决于`ctype`字段，`ctype`为不同的对象类型;`level`字段为日志级别。</br>
```sql
drop table directory_info;
create table if not exists directory_info(
    id int(4) AUTO_INCREMENT primary key,
    pid int(4) not null,
    is_leaf int(1) not null comment '1: 叶子节点，0: 目录',
    dtype int(1) not null comment '0: coordinator目录, 1: workflow目录',
    name varchar(128) not null,
    description varchar(1024)
);
```
#### 监控数据表
```sql
create table if not exists data_monitor(
    time_mark varchar(64) not null comment '时间字段，格式自定义',
    category varchar(64) not null comment '数据源分类',
    source_name varchar(64) not null comment '数据源名称',
    num double not null comment '监测值',
    min double comment '下限',
    max double comment '上限',
    workflow_instance_id varchar(8) not null, 
    remark varchar(1024) comment '备注'
);
```



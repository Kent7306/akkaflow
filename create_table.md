
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
create table workflow(
    name varchar(128) primary key  not null,
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    create_time datetime,
    last_update_time datetime
);
```

#### coordinator配置表
保存调度器信息</br>
```sql
drop table coordinator;
create table coordinator (
    name varchar(128) primary key,
    param JSON,
    dir varchar(128),
    content varchar(10240),
    cron varchar(128),
    depends JSON comment '工作流依赖id集合，例：[xxxx,xxxx]',
    workflow_names JSON comment '触发工作流name集合，例：[xxxx，xxxx]',
    stime datetime,
    etime datetime,
    is_enabled int,
    status int(1),
    description varchar(1024),
    create_time datetime,
    last_update_time datetime
);
```

#### node配置表
保存工作流节点信息</br>
```sql
drop table node;
create table node (
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
create table workflow_instance(
    id varchar(8) primary key not null,
    name varchar(128) not null,
    param JSON,
    status int(1),
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    stime datetime,
    etime datetime,
    create_time datetime,
    last_update_time datetime
);
```

#### coordinator触发记录表
记录调度器的触发情况，不过日志表中也有相关记录，该表暂时保留，后续会丰富信息。</br>
```sql
drop table coordinator_trigger_record;
create table coordinator_trigger_record (
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
create table node_instance (
    workflow_instance_id varchar(8) not null,
    name varchar(200) not null,
    is_action int(1) comment '是否为action节点',
    type varchar(100) comment '节点类型',
    content JSON comment '节点存放内容',
    description varchar(1024),
    status int(1),
    stime datetime,
    etime datetime,
    msg varchar(1024)
);
```

#### 日志表
各个阶段的执行日志会保存在该表当中。</br>
其中，`sid`字段可能为工作流实例id，节点id，这取决于`ctype`字段，`ctype`为不同的对象类型;`level`字段为日志级别。</br>
```sql
drop table log_record;
create table log_record (
    id int(10) primary key auto_increment,
    sid varchar(20),
    level varchar(10),
    ctype varchar(60),
    stime datetime,
    content varchar(1024)
);
```

####
各个阶段的执行日志会保存在该表当中。</br>
其中，`sid`字段可能为工作流实例id，节点id，这取决于`ctype`字段，`ctype`为不同的对象类型;`level`字段为日志级别。</br>
```sql
drop table directory_info;
create table directory_info(
    id int(4) AUTO_INCREMENT primary key,
    pid int(4) not null,
    is_leaf int(1) not null comment '1: 叶子节点，0: 目录',
    dtype int(1) not null comment '0: coordinator目录, 1: workflow目录',
    name varchar(128) not null,
    description varchar(1024)
);
```



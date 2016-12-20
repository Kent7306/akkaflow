set character_set_server = utf8;
set character_set_database = utf8;

1、workflow信息表
use wf;
drop table workflow;
create table workflow(
id varchar(8) primary key not null,
name varchar(128) not null,
description varchar(128),
mail_level JSON,
mail_receivers JSON,
create_time datetime,
last_update_time datetime
);

3、coordinator配置表
drop table coordinator;
create table coordinator (
id varchar(8) primary key not null,
name varchar(128) unique,
param JSON,
cron varchar(128),
depends JSON comment '工作流依赖id集合，例：xxxx,xxxx',
workflow_ids JSON comment '触发工作流id集合，例：xxxx，xxxx',
stime datetime,
etime datetime,
status int(1),
description varchar(1024),
create_time datetime,
last_update_time datetime
);

新node配置表
drop table node;
create table node (
name varchar(128) not null,
is_action int(1) comment '是否为action节点',
type varchar(100) comment '节点类型',
content JSON comment '节点存放内容',
workflow_id varchar(8) comment '外键->workflow_info:id',
description varchar(1024)
)	

2、workflow实例表
drop table workflow_instance;
create table workflow_instance(
id varchar(8) primary key not null,
workflow_id varchar(6) not null,
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
)



4、coordinator触发记录表
create table wf.coordinator_trigger_record (
id varchar(8) primavite key, --自增
param varchar(1024), -- json
trigger_time datetime,
coordinator_info_id varchar(8), --forger key
)



6、node实例表
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
)

7、日志表
create table log_record (
id int(10) primary key auto_increment,
sid varchar(20),
level varchar(10),
ctype varchar(60),
stime datetime,
content varchar(1024)
)

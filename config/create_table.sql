set character_set_server = utf8;
set character_set_database = utf8;
create table if not exists workflow(
    name varchar(128) primary key  not null,
    dir varchar(128),
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    instance_limit int,
    create_time datetime,
    last_update_time datetime
);

create table if not exists coordinator (
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
create table if not exists node (
    name varchar(128) not null,
    is_action int(1) comment '是否为action节点',
    type varchar(100) comment '节点类型',
    content JSON comment '节点存放内容',
    workflow_name varchar(128) comment '外键->workflow_info:name',
    description varchar(1024)
);    
create table if not exists workflow_instance(
    id varchar(8) primary key not null,
    name varchar(128) not null,
    dir varchar(128),
    param JSON,
    status int(1),
    description varchar(128),
    mail_level JSON,
    mail_receivers JSON,
    instance_limit int,
    stime datetime,
    etime datetime,
    create_time datetime,
    last_update_time datetime
);
create table if not exists coordinator_trigger_record (
    id varchar(8) primary key comment '标识',
    coordinator_name varchar(128),
    workflow_name varchar(128),
    param JSON,
    trigger_time datetime
);
create table if not exists node_instance (
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

create table if not exists log_record (
    id int(10) primary key auto_increment,
    sid varchar(20),
    level varchar(10),
    ctype varchar(60),
    stime datetime,
    content varchar(1024)
);

create table if not exists directory_info(
    id int(4) AUTO_INCREMENT primary key,
    pid int(4) not null,
    is_leaf int(1) not null comment '1: 叶子节点，0: 目录',
    dtype int(1) not null comment '0: coordinator目录, 1: workflow目录',
    name varchar(128) not null,
    description varchar(1024)
);
truncate node;
truncate workflow;
truncate directory_info;
truncate coordinator;
delete from node_instance where workflow_instance_id in (select id from workflow_instance where status = 1);
delete from workflow_instance where status = 1

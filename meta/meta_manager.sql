CREATE TABLE `lu_ds_instance`
(
    `id`            bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `instance_name` varchar(255)    NOT NULL DEFAULT '' COMMENT '数据源名称',
    `instance_type` tinyint         NOT NULL DEFAULT '0' COMMENT '数据源类型',
    `instance_info` text            NULL     DEFAULT NULL COMMENT '数据源连接信息',
    `create_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_ds_name`(`instance_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-实例信息';

CREATE TABLE `lu_ds_node`
(
    `id`            bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `instance_name` varchar(255)    NOT NULL DEFAULT '' COMMENT '数据源名称',
    `schema_name`   varchar(255)    NOT NULL DEFAULT '' COMMENT '模式名(对于没有模式概念的数据源则为null)',
    `table_name`    varchar(255)    NOT NULL DEFAULT '' COMMENT '表名',
    `create_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_ds_name_schema_table`(`instance_name`, `schema_name`, `table_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-节点信息';

CREATE TABLE `lu_ds_variable`
(
    `id`             bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `variable_name`  varchar(255)    NOT NULL DEFAULT '' COMMENT '变量名',
    `variable_value` text            NULL     DEFAULT NULL COMMENT '变量值',
    `create_time`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_variable`(`variable_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-变量信息';

CREATE TABLE `lu_ds_role`
(
    `id`            bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `role_name`     varchar(255)    NOT NULL DEFAULT '' COMMENT '角色名',
    `role_password` varchar(255)    NULL     DEFAULT NULL COMMENT '角色密码的MD5值',
    `parent_role`   varchar(255)    NULL     DEFAULT NULL COMMENT '继承的角色名(如果没有继承则为null)',
    `create_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_role`(`role_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-角色信息';

CREATE TABLE `lu_ds_role_instance`
(
    `id`                   bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `role_id`              bigint          NOT NULL DEFAULT '0' COMMENT '角色ID(lu_ds_role表主键)',
    `instance_id`          bigint          NOT NULL DEFAULT '0' COMMENT '实例ID(lu_ds_instance表主键)',
    `permission`           tinyint         NOT NULL DEFAULT '0' COMMENT '角色权限:0=无权限,1=可查看,2=可编辑,3=可管理',
    `custom_instance_info` text            NULL     DEFAULT NULL COMMENT '角色特有连接信息(覆盖实例默认信息)',
    `create_time`          datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`          datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_role_instance`(`role_id`, `instance_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-角色实例关联表';

CREATE TABLE `lu_ds_role_variable`
(
    `id`             bigint UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，不代表业务含义',
    `role_id`        bigint          NOT NULL DEFAULT '0' COMMENT '角色ID(lu_ds_role表主键)',
    `variable_id`    bigint          NOT NULL DEFAULT '0' COMMENT '变量ID(lu_ds_variable表主键)',
    `variable_value` text            NULL     DEFAULT NULL COMMENT '角色特有变量值(覆盖默认变量值)',
    `create_time`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`    datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更改时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_role_variable`(`role_id`, `variable_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '数据源管理-角色变量关联表';

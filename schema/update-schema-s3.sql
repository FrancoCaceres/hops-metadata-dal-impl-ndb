CREATE TABLE `hdfs_s3object_deletables` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `region` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `bucket` varchar(63) COLLATE latin1_general_cs DEFAULT NULL,
  `key` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `version_id` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  `reschedule_at` bigint(20) DEFAULT NULL,
  `scheduled_for` varchar(128) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_scheduled_for` (`scheduled_for`),
  KEY `idx_reschedule_at` (`reschedule_at`)
) ENGINE=ndbcluster AUTO_INCREMENT=51 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (id) */;

CREATE TABLE `hdfs_s3object_infos` (
  `inode_id` bigint(20) NOT NULL,
  `object_id` bigint(20) NOT NULL,
  `object_index` int(11) DEFAULT NULL,
  `region` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `bucket` varchar(63) COLLATE latin1_general_cs DEFAULT NULL,
  `key` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `version_id` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  `checksum` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`object_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */;

CREATE TABLE `hdfs_s3object_lookup_table` (
  `object_id` bigint(20) NOT NULL,
  `inode_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`object_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (object_id) */;

CREATE TABLE `hdfs_s3processables` (
  `inode_id` bigint(20) NOT NULL,
  `reschedule_at` bigint(20) DEFAULT NULL,
  `scheduled_for` varchar(128) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`inode_id`),
  KEY `idx_scheduled_for` (`scheduled_for`),
  KEY `idx_reschedule_at` (`reschedule_at`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1'
/*!50100 PARTITION BY KEY (inode_id) */;

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "VW_LOB_REBUILD" ("OWNER", "SEGMENT_NAME", "SEGMENT_TYPE", "COLUMN_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "TABLESPACE_NAME", "DEGREE", "MOVE_DDL", "RESTORE_PARALLEL") AS 
  SELECT 
    table_owner owner, 
    table_name segment_name, 
    'LOB SUBPARTITION' segment_type,
    column_name,
    cast(null as varchar2(128)) partition_name,
    LOB_SUBPARTITION_NAME subpartition_name,
    tablespace_name,
    '1' degree,
    'ALTER TABLE "' || table_owner || '"."' || table_name || 
    '" MOVE SUBPARTITION ' || subpartition_name || 
    '  LOB ("' || column_name || '") STORE AS (TABLESPACE <TABLESPACE>) PARALLEL <PARALLEL>' move_ddl,
    cast(null as varchar2(128)) restore_parallel
FROM dba_lob_subpartitions
UNION ALL
SELECT 
    table_owner, 
    table_name, 
    'LOB PARTITION' segment_type,
    column_name,
    lob_partition_name partition_name,
    cast(null as varchar2(128)) subpartition_name,
    tablespace_name,
    '1' degree,
    'ALTER TABLE "' || table_owner || '"."' || table_name || 
    '" MOVE PARTITION ' || partition_name || 
    '  LOB ("' || column_name || '") STORE AS (TABLESPACE <TABLESPACE>) PARALLEL <PARALLEL>',
    cast(null as varchar2(128)) restore_parallel
FROM dba_lob_partitions 
WHERE (table_owner, table_name) NOT IN (SELECT table_owner, table_name FROM dba_lob_subpartitions)
UNION ALL
SELECT 
    owner, 
    table_name, 
    'LOB' segment_type,
    column_name,
    cast(null as varchar2(128)) partition_name,
    cast(null as varchar2(128)) subpartition_name,
    tablespace_name,
    '1' degree,
    'ALTER TABLE "' || owner || '"."' || table_name || 
    '" MOVE LOB ("' || column_name || '") STORE AS (TABLESPACE <TABLESPACE>) PARALLEL <PARALLEL>',
    cast(null as varchar2(128)) restore_parallel
FROM dba_lobs 
WHERE partitioned = 'NO'
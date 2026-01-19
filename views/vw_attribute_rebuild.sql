  CREATE OR REPLACE FORCE EDITIONABLE VIEW "VW_ATTRIBUTE_REBUILD" ("OWNER", "SEGMENT_NAME", "SEGMENT_TYPE", "PARTITION_NAME", "SUBPARTITION_NAME", "COLUMN_NAME", "TABLESPACE_NAME", "DEGREE", "MOVE_DDL", "RESTORE_PARALLEL") AS 
  SELECT            
  owner, table_name segment_name, 'TABLE ATTRIBUTES' segment_type, 
  null partition_name,
  null subpartition_name,
  null column_name,
  DEF_TABLESPACE_NAME tablespace_name,
  null degree,
  'ALTER TABLE "' || T.OWNER || '"."' || T.table_name || '" MODIFY DEFAULT ATTRIBUTES TABLESPACE <TABLESPACE>' move_ddl,
  cast(null as varchar2(128)) restore_parallel
 FROM dba_part_tables T
WHERE NOT EXISTS
  (
     SELECT 1 
       FROM DBA_RECYCLEBIN R 
      WHERE R.OWNER=t.OWNER 
        AND R.object_name=t.table_name
  )
UNION ALL
SELECT            
  owner, index_name, 'INDEX ATTRIBUTES', 
  null partition_name,
  null subpartition_name,
  null column_name,
  DEF_TABLESPACE_NAME tablespace_name,
  null degree,
  'ALTER INDEX "' || I.OWNER || '"."' || I.index_name || '" MODIFY DEFAULT ATTRIBUTES TABLESPACE <TABLESPACE>',
  cast(null as varchar2(128)) restore_parallel
 FROM dba_part_indexes I
  WHERE NOT EXISTS 
  (
      SELECT 1
        FROM dba_lobs l
       WHERE l.owner = i.owner
         AND l.index_name = i.index_name
  )      
  AND NOT EXISTS
  (
     SELECT 1 
       FROM DBA_RECYCLEBIN R 
      WHERE R.OWNER=i.OWNER 
        AND R.object_name=i.index_name
  )
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "VW_INDEX_REBUILD" ("OWNER", "SEGMENT_NAME", "SEGMENT_TYPE", "COLUMN_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "TABLESPACE_NAME", "DEGREE", "MOVE_DDL", "RESTORE_PARALLEL") AS 
  SELECT 
   s.owner,
   s.segment_name,
   s.segment_type,
   cast(null as varchar2(128)) column_name,
   decode(segment_type, 'INDEX PARTITION', s.partition_name, NULL) partition_name,
   decode(segment_type, 'INDEX SUBPARTITION', s.partition_name, NULL) subpartition_name,
   s.tablespace_name,
   i.degree,
   case
     when s.segment_type = 'INDEX' THEN
        'ALTER INDEX "' || s.owner || '"."' || s.segment_name
        || '" REBUILD TABLESPACE <TABLESPACE>' 
        || ' PARALLEL <PARALLEL>'
     when s.segment_type = 'INDEX PARTITION' THEN
        'ALTER INDEX "' || s.owner || '"."' || s.segment_name
        || '" REBUILD PARTITION ' || s.partition_name
        || ' TABLESPACE <TABLESPACE>' 
        || ' PARALLEL <PARALLEL>'
     when s.segment_type = 'INDEX SUBPARTITION' THEN
        'ALTER INDEX "' || s.owner || '"."' || s.segment_name
        || '" REBUILD SUBPARTITION ' || s.partition_name
        || ' TABLESPACE <TABLESPACE>' 
        || ' PARALLEL <PARALLEL>'
   end move_ddl,
  CASE
    WHEN S.segment_type = 'INDEX' THEN
      CASE
        WHEN i.degree IS NULL OR UPPER(i.degree) IN ('0')
        THEN
          'ALTER INDEX "' || S.OWNER || '"."' || S.segment_name || '" NOPARALLEL'
        ELSE
          'ALTER INDEX "' || S.OWNER || '"."' || S.segment_name || '" PARALLEL (DEGREE ' || i.degree||')'
      END
  END restore_parallel
FROM   dba_segments s, dba_indexes i
WHERE  s.segment_type IN ('INDEX','INDEX PARTITION','INDEX SUBPARTITION')
   AND S.OWNER=i.OWNER
   AND S.segment_name=i.index_name    
  AND NOT EXISTS 
  (
      SELECT 1
        FROM   dba_lobs l
       WHERE  l.owner = s.owner
         AND    l.index_name = s.segment_name
  )      
  AND NOT EXISTS
  (
     SELECT 1 
       FROM DBA_RECYCLEBIN R 
      WHERE R.OWNER=S.OWNER 
        AND R.object_name=S.segment_name
  )
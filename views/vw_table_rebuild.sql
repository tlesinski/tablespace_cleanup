  CREATE OR REPLACE FORCE EDITIONABLE VIEW "VW_TABLE_REBUILD" ("OWNER", "SEGMENT_NAME", "SEGMENT_TYPE", "COLUMN_NAME", "PARTITION_NAME", "SUBPARTITION_NAME", "TABLESPACE_NAME", "DEGREE", "MOVE_DDL", "RESTORE_PARALLEL") AS 
  SELECT 
 s.owner,
 s.segment_name,
 s.segment_type,
 cast(null as varchar2(128)) column_name,
 decode(segment_type, 'TABLE PARTITION', s.partition_name, NULL) partition_name,
 decode(segment_type, 'TABLE SUBPARTITION', s.partition_name, NULL) subpartition_name,
 s.tablespace_name,
 t.degree,
 case
   when s.segment_type = 'TABLE' THEN
      'ALTER TABLE "' || s.owner || '"."' || s.segment_name
      || '" MOVE TABLESPACE <TABLESPACE>' 
      || ' PARALLEL <PARALLEL>'
   when s.segment_type = 'TABLE PARTITION' THEN
      'ALTER TABLE "' || s.owner || '"."' || s.segment_name
      || '" MOVE PARTITION ' || s.partition_name
      || ' TABLESPACE <TABLESPACE>' 
      || ' PARALLEL <PARALLEL>'
   when s.segment_type = 'TABLE SUBPARTITION' THEN
      'ALTER TABLE "' || s.owner || '"."' || s.segment_name
      || '" MOVE SUBPARTITION ' || s.partition_name
      || ' TABLESPACE <TABLESPACE>' 
      || ' PARALLEL <PARALLEL>'
 end move_ddl,
  CASE
    WHEN S.segment_type = 'TABLE' THEN
      CASE
        WHEN t.degree IS NULL OR UPPER(t.degree) IN ('0')
        THEN
          'ALTER TABLE "' || S.OWNER || '"."' || S.segment_name || '" NOPARALLEL'
        ELSE
          'ALTER TABLE "' || S.OWNER || '"."' || S.segment_name || '" PARALLEL (DEGREE ' || t.degree||')'
      END
  END restore_parallel
FROM dba_segments s, dba_tables t
WHERE s.segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION')
 AND S.OWNER=T.OWNER
 AND S.segment_name=T.table_name    
 AND NOT EXISTS(SELECT 1 FROM DBA_RECYCLEBIN R WHERE R.OWNER=S.OWNER AND R.object_name=S.segment_name)
 and (t.iot_type IS NULL OR t.iot_type!='IOT_OVERFLOW');
 /
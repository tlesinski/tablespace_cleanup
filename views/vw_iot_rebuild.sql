CREATE OR REPLACE FORCE EDITIONABLE VIEW "VW_IOT_OVERFLOW_REBUILD"
AS
SELECT
    s.owner,
    s.segment_name,
    s.segment_type,
    -- Dla IOT OVERFLOW potrzebujemy nazwy tabeli głównej
    t.table_name AS parent_table_name,
    DECODE(s.segment_type,'TABLE PARTITION',s.partition_name,null) AS partition_name,
    DECODE(s.segment_type,'TABLE SUBPARTITION',s.partition_name,null) AS subpartition_name,
    s.tablespace_name,
    t.degree,
    CASE
        -- 1. Zwykły OVERFLOW (niepartycjonowany)
        WHEN s.segment_type = 'TABLE'
         AND t.iot_type = 'IOT_OVERFLOW'
        THEN
            'ALTER TABLE ' || s.owner || '."' || t.iot_name ||
            '" MOVE OVERFLOW TABLESPACE <TABLESPACE> PARALLEL <PARALLEL>'
        -- 2. Partycja OVERFLOW
        WHEN s.segment_type = 'TABLE PARTITION'
         AND t.iot_type = 'IOT_OVERFLOW'
        THEN
            'ALTER TABLE ' || s.owner || '."' || t.iot_name ||
            '" MOVE PARTITION ' || s.partition_name ||
            ' OVERFLOW TABLESPACE <TABLESPACE> PARALLEL <PARALLEL>'
        -- 3. Subpartycja OVERFLOW
        WHEN s.segment_type = 'TABLE SUBPARTITION'
         AND t.iot_type = 'IOT_OVERFLOW'
        THEN
            'ALTER TABLE ' || s.owner || '."' || t.iot_name ||
            '" MOVE SUBPARTITION ' || s.partition_name ||
            ' OVERFLOW TABLESPACE <TABLESPACE> PARALLEL <PARALLEL>'
    END AS move_ddl,
    -- Przywrócenie równoległości na tabeli głównej (IOT)
    'ALTER TABLE ' || s.owner || '."' || t.iot_name || '" ' ||
    CASE
        WHEN t.degree IS NULL
          OR UPPER(TRIM(t.degree)) IN ('0', '1', 'DEFAULT')
        THEN 'NOPARALLEL'
        ELSE 'PARALLEL ' || t.degree
    END AS restore_parallel
FROM dba_segments s
JOIN dba_tables t
  ON s.owner = t.owner
 AND s.segment_name = t.table_name
WHERE t.iot_type = 'IOT_OVERFLOW'
  AND s.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION');

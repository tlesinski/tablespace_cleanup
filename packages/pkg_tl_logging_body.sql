CREATE OR REPLACE PACKAGE BODY "PKG_TL_LOGGING" AS
-- Package : PKG_TL_LOGGING --
-- Developer : --
-- Date : 2020-02-24 --
-- Purpose : Logging package --
-- Prerequisite : MD_PROCESS_LOG, MD_PROCESS_LOG_SEQ data model --

-- Change History --
-- --
-- Version Date Programmer Description --

-- 1.0 2020-02-20 Tomasz, Lesinski Initial version --
-- 2.0 2021-12-01 Tomasz, Lesinski Simplification --

--Procedure pro_init_log just pre declaration

PROCEDURE pro_init_log
(
p_force IN VARCHAR2 DEFAULT 'N'
);

PROCEDURE prc_log_cleanup_table;

--Procedure prc_log
-- base logging message

PROCEDURE prc_log
(
p_log_id IN NUMBER,
p_log_msg IN CLOB,
p_log_lvl IN VARCHAR2 DEFAULT 'N',
p_mstr_log_id IN NUMBER DEFAULT NULL,
p_log_categ IN VARCHAR2 DEFAULT NULL,
p_mstr_fun IN VARCHAR2 DEFAULT NULL,
p_log_sttus IN VARCHAR2 DEFAULT NULL,
p_start_date IN DATE DEFAULT NULL,
p_end_date IN DATE DEFAULT NULL,
p_last_err_code IN VARCHAR2 DEFAULT NULL,
p_last_err_desc IN VARCHAR2 DEFAULT NULL
)
IS
PRAGMA autonomous_transaction;
l_curr_schema VARCHAR2(128) := sys_context('USERENV', 'CURRENT_SCHEMA');
l_today VARCHAR2(10) := TO_CHAR(SYSDATE, 'YYYYMMDD');
l_log_categ VARCHAR2(128);
l_msg CLOB;
l_sysdate VARCHAR2(64);
l_sql CLOB;
l_mstr_log_id NUMBER;
l_log_id NUMBER;
BEGIN
l_sysdate := to_char(SYSDATE, g_date_formt_const, 'nls_date_language = ''' || g_lang_const || '''' );

--daily log categ
l_log_categ := 'LOG_' || l_curr_schema || '_' || l_today ||'.log';

l_msg := to_clob(chr(10) || l_sysdate || ' | ' ) || p_log_msg || to_clob(chr(10));

IF p_log_lvl = 'N' OR ( p_log_lvl = 'D' AND g_log_lvl = 'D' ) THEN
  IF p_log_id IS NOT NULL THEN
    l_log_id := 0;

    l_sql := 'MERGE INTO ' || g_log_table_name || q'[ a
    USING
    (
      SELECT
         nvl(:p_mstr_log_id, :p_log_id)        AS mstr_log_id
        ,:p_log_id                             AS log_id
        ,:p_log_categ                          AS log_categ
        ,:p_mstr_fun                           AS mstr_fun
        ,:p_log_sttus                          AS log_sttus
        ,:p_start_date                         AS start_date
        ,:p_end_date                           AS end_date
        ,:p_log_msg                            AS log_msg
        ,:p_last_err_code                      AS last_err_code
        ,:p_last_err_desc                      AS last_err_desc
        ,sys_context('userenv','instance')     AS inst_id
        ,sys_context('userenv','sid')          AS sid
        ,sys_context('userenv','sessionid')    AS serial#
        ,sys_context('userenv','client_info')  AS client_info
        ,sys_context('userenv','module')       AS module
      FROM dual
    ) b
    ON
    (
      a.mstr_log_id = b.mstr_log_id
      AND a.log_id = b.log_id
    )
    WHEN MATCHED THEN
      UPDATE
      SET
        a.log_categ     = nvl(b.log_categ, a.log_categ),
        a.mstr_fun      = nvl(b.mstr_fun, a.mstr_fun),
        a.log_sttus     = nvl(b.log_sttus, a.log_sttus),
        a.start_date    = nvl(b.start_date, a.start_date),
        a.end_date      = nvl(b.end_date, a.end_date),
        a.log_msg       = a.log_msg || b.log_msg,
        a.last_err_code = nvl(b.last_err_code, a.last_err_code),
        a.last_err_desc = nvl(b.last_err_desc, a.last_err_desc),
        a.last_updt_dt  = SYSDATE,
        a.last_updt_by  = sys_context('USERENV','SESSION_USER')
    WHEN NOT MATCHED THEN
      INSERT
      (
         mstr_log_id
        ,log_id
        ,log_categ
        ,mstr_fun
        ,log_sttus
        ,start_date
        ,end_date
        ,log_msg
        ,last_err_code
        ,last_err_desc
        ,inst_id
        ,sid
        ,serial#
        ,client_info
        ,module
        ,crtn_dt
        ,crtn_by
        ,last_updt_dt
        ,last_updt_by
      )
      VALUES
      (
         b.mstr_log_id
        ,b.log_id
        ,b.log_categ
        ,b.mstr_fun
        ,nvl(b.log_sttus, :g_sttus_undefined_const)
        ,nvl(b.start_date, SYSDATE)
        ,b.end_date
        ,b.log_msg
        ,b.last_err_code
        ,b.last_err_desc
        ,b.inst_id
        ,b.sid
        ,b.serial#
        ,b.client_info
        ,b.module
        ,SYSDATE
        ,sys_context('USERENV','SESSION_USER')
        ,SYSDATE
        ,sys_context('USERENV','SESSION_USER')
      )]';

    BEGIN
      EXECUTE IMMEDIATE l_sql
      USING
         p_mstr_log_id
        ,p_log_id
        ,p_log_id
        ,p_log_categ
        ,p_mstr_fun
        ,p_log_sttus
        ,p_start_date
        ,p_end_date
        ,l_msg
        ,p_last_err_code
        ,p_last_err_desc
        ,pkg_tl_logging.g_sttus_undefined_const;

      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        pro_init_log( p_force => 'Y' );

        EXECUTE IMMEDIATE l_sql
        USING
           p_mstr_log_id
          ,p_log_id
          ,p_log_id
          ,p_log_categ
          ,p_mstr_fun
          ,p_log_sttus
          ,p_start_date
          ,p_end_date
          ,l_msg
          ,p_last_err_code
          ,p_last_err_desc
          ,pkg_tl_logging.g_sttus_undefined_const;

        COMMIT;
    END;
  END IF;
END IF;

END prc_log;

--Procedure prc_error_stack
-- dumps oracle stack if error occures into table MD_PRCSS_LOG

PROCEDURE prc_error_stack
(
p_log_id IN NUMBER,
p_mstr_log_id IN NUMBER DEFAULT NULL
)
IS
l_sqlcode NUMBER;
l_sqlerrm VARCHAR2(255);
l_message CLOB;
BEGIN
l_sqlcode := sqlcode;
l_sqlerrm := substr(SQLERRM, 1, 255);

l_message := chr(10) ||
  '===================================================================' || chr(10) ||
  ' SQLCODE:         ' || l_sqlcode                                     || chr(10) ||
  ' SQLERRM:         ' || l_sqlerrm                                     || chr(10) ||
  '===================================================================' || CHR(10) ||
  ' Call stack:      ' || dbms_utility.format_call_stack()              || CHR(10) ||
  '===================================================================' || chr(10) ||
  ' Error stack:     ' || dbms_utility.format_error_stack()             || CHR(10) ||
  '===================================================================' || CHR(10) ||
  ' Error backtrace: ' || dbms_utility.format_error_backtrace           || CHR(10) ||
  '===================================================================';

dbms_output.put_line(l_message);

prc_log
(
  p_mstr_log_id    => p_mstr_log_id,
  p_log_id         => p_log_id,
  p_log_sttus      => pkg_tl_logging.g_sttus_error_const,
  p_end_date       => SYSDATE,
  p_log_msg        => l_message,
  p_last_err_code  => l_sqlcode,
  p_last_err_desc  => l_sqlerrm
);

END prc_error_stack;

-- creates local log table md_process_log
-- and local log sequence md_process_log_seq

PROCEDURE prc_log_create_table
IS
l_check NUMBER;
l_sql CLOB;
l_proc_name VARCHAR2(128) := 'prc_log_create_table';
BEGIN
--------------------------------------------
--create local log table g_log_table_name
--------------------------------------------
SELECT COUNT(*)
INTO l_check
FROM user_tables
WHERE table_name = g_log_table_name;

SELECT COUNT(*)
  INTO l_check
FROM (
  SELECT 1
    FROM user_tables
   WHERE table_name = g_log_table_name
   UNION ALL
  SELECT 1
    FROM user_synonyms
   WHERE synonym_name = g_log_table_name
);

IF l_check = 0 THEN
  dbms_output.put_line('Try to create log table ' || g_log_table_name);

  l_sql := q'!CREATE TABLE ""MD_PROCESS_LOG""

( ""MSTR_LOG_ID"" NUMBER NOT NULL ENABLE,
""LOG_ID"" NUMBER NOT NULL ENABLE,
""LOG_CATEG"" VARCHAR2(256 BYTE),
""MSTR_FUN"" VARCHAR2(128 BYTE),
""LOG_STTUS"" VARCHAR2(30 BYTE) NOT NULL ENABLE,
""START_DATE"" DATE,
""END_DATE"" DATE,
""INTERVAL_TIME"" INTERVAL DAY (2) TO SECOND (6) GENERATED ALWAYS AS (NUMTODSINTERVAL(""END_DATE""-""START_DATE"",'DAY')) VIRTUAL ,
""LOG_MSG"" CLOB NOT NULL ENABLE,
""LAST_ERR_CODE"" VARCHAR2(30 BYTE),
""LAST_ERR_DESC"" VARCHAR2(256 BYTE),
""INST_ID"" NUMBER,
""SID"" NUMBER,
""SERIAL#"" NUMBER,
""CLIENT_INFO"" VARCHAR2(128 BYTE),
""MODULE"" VARCHAR2(128 BYTE),
""CRTN_DT"" DATE DEFAULT SYSDATE NOT NULL ENABLE,
""CRTN_BY"" VARCHAR2(128 BYTE) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL ENABLE,
""LAST_UPDT_DT"" DATE DEFAULT SYSDATE NOT NULL ENABLE,
""LAST_UPDT_BY"" VARCHAR2(128 BYTE) DEFAULT sys_context('USERENV','SESSION_USER') NOT NULL ENABLE
)
PARTITION BY RANGE (""START_DATE"") INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION ""P_START""  VALUES LESS THAN (TO_DATE(' 2019-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')) )!';

EXECUTE IMMEDIATE l_sql;

l_sql := 'CREATE INDEX ""MD_PROCESS_LOG_IDX1"" ON ""MD_PROCESS_LOG"" (""MSTR_LOG_ID"", ""LOG_ID"") LOCAL';

  EXECUTE IMMEDIATE l_sql;
ELSE
  dbms_output.put_line('Local log table ' || g_log_table_name || ' or synonym already exists');
END IF;

--------------------------------------------
--create local log sequence g_log_sequence_name
--------------------------------------------
SELECT COUNT(*)
  INTO l_check
FROM (
  SELECT 1
    FROM user_sequences
   WHERE sequence_name = g_log_sequence_name
   UNION ALL
  SELECT 1
    FROM user_synonyms
   WHERE synonym_name = g_log_sequence_name
);

IF l_check = 0 THEN
  dbms_output.put_line('Try to create log sequence ' || g_log_sequence_name);

  l_sql := 'CREATE SEQUENCE md_process_log_seq START WITH 1 INCREMENT BY 1';

  EXECUTE IMMEDIATE l_sql;
ELSE
  dbms_output.put_line('Local log sequence ' || g_log_sequence_name || ' or synonym already exists');
END IF;

prc_log_cleanup_table;

EXCEPTION
WHEN OTHERS THEN
prc_error_stack(p_log_id=>null);

  raise_application_error( -20001, 'Creation of local log table ' || g_log_table_name || chr(10) ||
                                   ' or sequence ' || g_log_sequence_name || chr(10) ||
                                   ' failed turn on serveroutput to get more info' );

END prc_log_create_table;

--Procedure pro_init_log
-- initializes log in physical file and in md_process_log table

PROCEDURE pro_init_log
(
p_force IN VARCHAR2 DEFAULT 'N'
)
IS
l_check NUMBER;
BEGIN
dbms_output.put_line('pro_init_log');

IF g_curr_schema_name IS NULL OR p_force = 'Y' THEN
  g_curr_schema_name := sys_context('USERENV', 'CURRENT_SCHEMA');

  prc_log_create_table;
END IF;

END pro_init_log;

-- cleanup old partitions in table md_process_log

PROCEDURE prc_log_cleanup_table
IS
l_proc_name VARCHAR2(128) := 'prc_log_cleanup_table';
l_long VARCHAR2(4000);
l_interval_date DATE;
l_partition_date DATE;
l_sql CLOB;
BEGIN
dbms_output.put_line
(
'Calling procedure ' || $$plsql_unit|| '.' || l_proc_name
);

l_interval_date := (SYSDATE - ABS(g_log_retention_days));

dbms_output.put_line
(
  'Drop partitions for table ' || g_log_table_name || chr(10) ||
  '  older than ' || g_log_retention_days || ' days' || chr(10) ||
  '  older than ' || to_char( l_interval_date, g_date_formt_const )
);

FOR i IN ( SELECT table_name, partition_name, high_value
             FROM user_tab_partitions
            WHERE table_name = g_log_table_name
              AND partition_name != 'P_START'
            ORDER BY partition_position )
LOOP
  --convert partition high_value long to date;
  l_long := i.high_value;

  EXECUTE IMMEDIATE 'SELECT ' || l_long || ' FROM dual' INTO l_partition_date;

  IF l_interval_date > l_partition_date THEN
    l_sql := 'ALTER TABLE ' || i.table_name || ' DROP PARTITION ' || i.partition_name;

    dbms_output.put_line
    (
      'Partition ' || i.partition_name || ' high_value ' || to_char( l_partition_date, g_date_formt_const ) || chr(10) ||
      ' older than ' || to_char( l_interval_date, g_date_formt_const ) || chr(10) ||
      ' to drop: ' || l_sql
    );

    EXECUTE IMMEDIATE l_sql;
  END IF;
END LOOP;

dbms_output.put_line
(
  'Completed procedure ' || $$plsql_unit || '.' || l_proc_name
);

EXCEPTION
WHEN OTHERS THEN
dbms_output.put_line('Error cleaning log table ' || SQLCODE || substr(SQLERRM, 1, 200));
END prc_log_cleanup_table;
BEGIN

--calling base procedure to intialize entries in md_process_log table and file with directories

pro_init_log;
END PKG_TL_LOGGING;
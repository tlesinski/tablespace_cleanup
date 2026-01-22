CREATE OR REPLACE PACKAGE "PKG_TL_LOGGING" 
--AUTHID CURRENT_USER
authid current_user
AS
  /*
    ===============================================================================
    Package      : PKG_TL_LOGGING
    Version      : 2.1
    Developer    : Tomasz Lesinski
    Date         : 2026-01-22
    Purpose      : Logging and monitoring for process execution
    Prerequisite : MD_PROCESS_LOG, MD_PROCESS_LOG_SEQ data model
    ===============================================================================
    
    Change History:
    ===============================================================================
    Version    Date         Programmer         Description
    ===============================================================================
    1.0        2020-02-20   Tomasz Lesinski    Initial version
    2.0        2021-12-01   Tomasz Lesinski    Simplification
    2.1        2026-01-22   Tomasz Lesinski    Added version, date, developer info
    ===============================================================================
  */

  -- global variables

  g_lang_const CONSTANT VARCHAR2(30) := 'english';

  g_timestamp_formt_const CONSTANT VARCHAR2(30) := 'DD-MM-YYYY HH24:MI:SSXFF';
  g_date_formt_const CONSTANT VARCHAR2(30) := 'DD-MM-YYYY HH24:MI:SS';
  g_short_date_formt_const CONSTANT VARCHAR2(30) := 'DD-MM-YYYY';

  g_sttus_init_const CONSTANT VARCHAR2(30) := 'INIT';
  g_sttus_executing_const CONSTANT VARCHAR2(30) := 'EXECUTING';
  g_sttus_warning_const CONSTANT VARCHAR2(30) := 'WARNING';
  g_sttus_success_const CONSTANT VARCHAR2(30) := 'SUCCESS';
  g_sttus_error_const CONSTANT VARCHAR2(30) := 'ERROR';
  g_sttus_running_const CONSTANT VARCHAR2(30) := 'RUNNING';
  g_sttus_undefined_const CONSTANT VARCHAR2(30) := 'UNDEFINED';

  g_err_sev_low_const CONSTANT VARCHAR2(10) := 'LOW';
  g_err_sev_med_const CONSTANT VARCHAR2(10) := 'MEDIUM';
  g_err_sev_hig_const CONSTANT VARCHAR2(10) := 'HIGH';

  --Can be set to Y if there is a need for writing log entries of type debug during the next execution.
  --'N' normal mode 'D' debug mode
  g_log_lvl VARCHAR2(10) := 'N';

  --How many days should the log be stored. Used by prc_log_cleanup_table.
  g_log_retention_days NUMBER := 90;

  --The name of log table.
  g_log_table_name VARCHAR2(128) := 'MD_PROCESS_LOG';

  --The name of sequence used for PK.
  g_log_sequence_name VARCHAR2(128) := 'MD_PROCESS_LOG_SEQ';

  g_curr_schema_name VARCHAR2(128);

  --Procedure prc_log
  -- base logging message

  -- Parameters:
  -- p_log_id log_id of entry to be inserted or updated
  -- p_log_msg message to save (the only required parameter)
  -- p_log_lvl logging level 'N' - normal default | 'D' - debug level
  -- p_mstr_log_id mstr_log_id (group id) of entry to be inserted or updated
  -- p_log_categ log category description
  -- p_mstr_fun log function name
  -- p_log_sttus status of process execution
  -- p_start_date process start date
  -- p_end_date process end date
  -- p_last_err_code ORA error number
  -- p_last_err_desc error description

  -- Example use #1:
  -- pkg_tl_logging.prc_log( p_log_msg => 'log message');
  --
  -- Example use #2:
  -- DECLARE
  -- l_log_id NUMBER := md_process_log_seq.nextval;
  -- BEGIN
  -- pkg_tl_logging.prc_log
  -- (
  -- p_log_id => l_log_id,
  -- p_log_categ => 'load dimensions',
  -- p_mstr_fun => 'prc_load_time_dim',
  -- p_log_sttus => pkg_tl_logging.g_sttus_init_const,
  -- p_start_date => SYSDATE,
  -- p_log_msg => 'init message'
  -- );
  -- END;

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
  );

  --Procedure prc_error_stack
  -- Dumps oracle stack into table MD_PRCSS_LOG if error occurs.
  -- All parameters are optional. If log_id is used stack will be
  -- printed in auto log and passed log_id entry.

  -- Parameters:
  -- p_log_id log_id
  -- p_proc_name procedure name
  -- p_mstr_log_id used for custom logging entries (not auto)

  PROCEDURE prc_error_stack
  (
    p_log_id IN NUMBER,
    p_mstr_log_id IN NUMBER DEFAULT NULL
  );
END PKG_TL_LOGGING;
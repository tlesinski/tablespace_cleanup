create or replace PACKAGE pkg_tbs_rebuild IS
   /*
    ===============================================================================
    Package      : PKG_TBS_REBUILD
    Version      : 1.0
    Developer    : Tomasz Lesinski
    Date         : 2026-01-22
    Purpose      : Tablespace rebuild orchestration - plan & execute segment moves
    Description  : Plan and execute rebuild of segments from one tablespace to
                   another using restartable RUN + STEPS model. Create
                   independent runs for PRIMARY (TS migration), FIX_UNUSABLE
                   (rebuild unusable indexes), FIX_INVALID (compile objects),
                   and optionally drop empty datafiles after migration.
    ===============================================================================
    
    Change History:
    ===============================================================================
    Version    Date         Programmer         Description
    ===============================================================================
    1.0        2026-01-22   Tomasz Lesinski    Initial version
    ===============================================================================
   */

   ---------------------------------------------------------------------------
   -- PUBLIC TYPES / CONSTANTS (jeśli potrzebne na zewnątrz)
   ---------------------------------------------------------------------------
   g_status_new          CONSTANT VARCHAR2(20) := 'NEW';
   g_status_planned      CONSTANT VARCHAR2(20) := 'PLANNED';
   g_status_running      CONSTANT VARCHAR2(20) := 'RUNNING';
   g_status_finished_ok  CONSTANT VARCHAR2(20) := 'FINISHED_OK';
   g_status_finished_err CONSTANT VARCHAR2(20) := 'FINISHED_ERR';
   
   g_primary_const       CONSTANT VARCHAR2(20) := 'PRIMARY';
   
   ---------------------------------------------------------------------------
   -- PRIMARY RUN: migracja obiektów z TS_source -> TS_target
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_primary 
   (
      p_owner_filter      IN VARCHAR2  DEFAULT NULL,
      p_source_tablespace IN VARCHAR2,
      p_target_tablespace IN VARCHAR2,
      p_parallel_degree   IN PLS_INTEGER DEFAULT 4
   ) 
   RETURN NUMBER;

   ---------------------------------------------------------------------------
   -- FIX_UNUSABLE RUN: odbudowa UNUSABLE indeksów (bez powiązania z PRIMARY)
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_fix_unusable
   (
      p_owner_filter     IN VARCHAR2  DEFAULT NULL,
      p_parallel_degree  IN PLS_INTEGER DEFAULT 4
   ) RETURN NUMBER;

   ---------------------------------------------------------------------------
   -- FIX_INVALID RUN: kompilacja INVALID obiektów (bez powiązania z PRIMARY)
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_fix_invalid
   (
      p_owner_filter     IN VARCHAR2  DEFAULT NULL
   ) RETURN NUMBER;

   ---------------------------------------------------------------------------
   -- EXECUTION ENGINE: wykonanie kroków dla run_id
   --
   -- p_execute      - 'Y' = EXECUTE IMMEDIATE, 'N' = tylko log / update statusu
   -- p_stop_on_error- 'Y' = przerwij po pierwszym błędzie, 'N' = kontynuuj
   -- p_parallel_exec- jeśli nie NULL, użyj tego poziomu PARALLEL dla kroku
   --                  (do wstrzyknięcia do ddl_text); po operacji zostanie
   --                  wykonany ddl_text_orig_parallel jeśli nie jest NULL.
   ---------------------------------------------------------------------------
   PROCEDURE prc_process_run
   (
      p_run_id        IN NUMBER,
      p_execute       IN VARCHAR2 DEFAULT 'Y',
      p_stop_on_error IN VARCHAR2 DEFAULT 'N',
      p_parallel_exec IN PLS_INTEGER DEFAULT NULL
   );

   ---------------------------------------------------------------------------
   -- ROLLBACK RUN: wykonuje rollback_ddl_text dla kroków danego run_id
   -- (np. w odwrotnej kolejności step_order).
   ---------------------------------------------------------------------------
   PROCEDURE prc_rollback_run 
   (
      p_run_id        IN NUMBER,
      p_execute       IN VARCHAR2 DEFAULT 'Y',
      p_stop_on_error IN VARCHAR2 DEFAULT 'N'
   );

   ---------------------------------------------------------------------------
   -- DROP EMPTY DATAFILES:
   -- Próbuje zidentyfikować puste datafile w podanym TS i (opcjonalnie)
   -- wykonać ALTER TABLESPACE ... DROP DATAFILE ...
   --
   -- p_tablespace_name - nazwa TS
   -- p_execute         - 'N' = tylko raport/log, 'Y' = wykonaj ALTER
   ---------------------------------------------------------------------------
   PROCEDURE prc_drop_empty_datafiles 
   (
      p_tablespace_name IN VARCHAR2,
      p_execute         IN VARCHAR2 DEFAULT 'N'
   );

END pkg_tbs_rebuild;
/
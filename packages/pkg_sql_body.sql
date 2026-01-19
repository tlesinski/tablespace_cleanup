CREATE OR REPLACE PACKAGE BODY PKG_SQL AS
  /*
    Package      : PKG_SQL
    Developer    : 
    Date         : 2024-10-30
    Purpose      : Sql package

    Prerequisite : MD_PROCESS_LOG, MD_PROCESS_LOG_SEQ data model

    Change History:
    ------------------------------------------------------------------------------
    Version    Date         Programmer         Description
    ------------------------------------------------------------------------------
    1.0        2024-10-30   Tomasz Lesinski    Initial version
  */

  /*
    FUNCTION fn_run_into_sql
    Purpose:
      Executes a dynamic SQL query (provided as CLOB) and retrieves a single numeric 
      result (e.g., COUNT). If executed in ""read mode,"" the function only logs the 
      SQL without execution.
  */
  FUNCTION fn_run_into_sql
  ( 
    p_log_id  IN NUMBER,
    p_sql     IN CLOB,
    p_execute IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER 
  IS
    l_cnt NUMBER := NULL;
  BEGIN
    pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => p_sql);

    IF p_execute = 'Y' THEN
      EXECUTE IMMEDIATE p_sql INTO l_cnt;
    ELSE
      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'SQL execution skipped (read mode)');
    END IF;

    RETURN l_cnt;
  END fn_run_into_sql;

  /*
    FUNCTION fn_run_sql
    Purpose:
      Executes a dynamic SQL statement (provided as CLOB) and optionally logs the 
      SQL text and affected row count. If executed in ""read mode,"" the function only 
      logs the SQL without execution.
  */
  FUNCTION fn_run_sql
  ( 
    p_log_id  IN NUMBER,
    p_sql     IN CLOB,
    p_execute IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER 
  IS
    l_cnt NUMBER := NULL;
  BEGIN
    -- Log the SQL statement
    pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => p_sql);

    -- Execute or log-only based on p_execute
    IF p_execute = 'Y' THEN
      EXECUTE IMMEDIATE p_sql;

      l_cnt := SQL%ROWCOUNT;

      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'Rows processed: ' || l_cnt);
    ELSE
      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'SQL execution skipped (read mode)');
    END IF;

    RETURN l_cnt;
  END fn_run_sql;

  -- Function to execute SQL with individual binds and optional read-only mode
  FUNCTION fn_run_sql_in_bind
  (
    p_log_id      IN NUMBER,
    p_sql         IN CLOB,
    p_array_bind  IN SYS.ODCIVARCHAR2LIST, -- List of bind variables
    p_execute IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER 
  IS
    l_cnt NUMBER;
    l_sql CLOB; -- Variable to store the final SQL with binds applied
  BEGIN
    -- Log the SQL statement and bind array details
    pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => p_sql);

    FOR i IN 1 .. p_array_bind.COUNT 
    LOOP
      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'Bind ' || i || ': ' || p_array_bind(i));
    END LOOP;

    -- If read-only, only log without execution
    IF p_execute = 'Y' THEN
      -- Dynamically prepare SQL with bind variables
      l_sql := p_sql;

      -- Execute the SQL with individual binds
      CASE p_array_bind.COUNT
        WHEN 0 THEN EXECUTE IMMEDIATE l_sql;
        WHEN 1 THEN EXECUTE IMMEDIATE l_sql USING p_array_bind(1);
        WHEN 2 THEN EXECUTE IMMEDIATE l_sql USING p_array_bind(1), p_array_bind(2);
        WHEN 3 THEN EXECUTE IMMEDIATE l_sql USING p_array_bind(1), p_array_bind(2), p_array_bind(3);
        WHEN 4 THEN EXECUTE IMMEDIATE l_sql USING p_array_bind(1), p_array_bind(2), p_array_bind(3), p_array_bind(4);
        WHEN 5 THEN EXECUTE IMMEDIATE l_sql USING p_array_bind(1), p_array_bind(2), p_array_bind(3), p_array_bind(4), p_array_bind(5);
        -- Extend this CASE as needed for more binds
        ELSE
          pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'Too many bind variables (max 5 supported)');

          raise_application_error(-20001, 'Exceeded maximum bind variables limit');
      END CASE;

      l_cnt := SQL%ROWCOUNT;

      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'Rows processed: ' || l_cnt);
    ELSE
      pkg_tl_logging.prc_log(p_log_id => p_log_id, p_log_msg => 'SQL execution skipped (read mode)');
    END IF;

    RETURN l_cnt;
  END fn_run_sql_in_bind;
END PKG_SQL;
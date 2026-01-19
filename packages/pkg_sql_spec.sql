CREATE OR REPLACE PACKAGE PKG_SQL AS 
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

    Parameters:
      p_log_id  IN NUMBER    - Unique identifier for logging the SQL statement.
      p_sql     IN CLOB      - The SQL query to execute or log.
      p_execute IN VARCHAR2  - Controls execution.
                               If 'Y' (default), the SQL statement is executed.
                               If 'N', the SQL is logged without execution.

    Returns:
      NUMBER - The result of the SQL query if executed, NULL if in read mode.
  */
  FUNCTION fn_run_into_sql
  ( 
    p_log_id  IN NUMBER,
    p_sql     IN CLOB,
    p_execute IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER; 

  /*
    FUNCTION fn_run_sql
    Purpose:
      Executes a dynamic SQL statement (provided as CLOB) and optionally logs the 
      SQL text and affected row count. If executed in ""read mode,"" the function only 
      logs the SQL without execution.

    Parameters:
      p_log_id  IN NUMBER    - Unique identifier for logging the SQL statement.
      p_sql     IN CLOB      - The SQL statement to execute or log.
      p_execute IN VARCHAR2  - Controls execution. 
                               If 'Y' (default), the SQL statement is executed.
                               If 'N', the SQL is logged without execution.

    Returns:
      NUMBER - Row count if executed, NULL if in read mode.
  */
  FUNCTION fn_run_sql
  ( 
    p_log_id  IN NUMBER,
    p_sql     IN CLOB,
    p_execute IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER;

  /*
    FUNCTION fn_run_sql_in_bind
    Purpose:
      Executes a dynamic SQL statement with a specified list of bind variables, allowing 
      optional logging in ""read mode."" Supports a maximum of 5 bind variables.

    Parameters:
      p_log_id     IN NUMBER               - Unique identifier for logging the SQL statement.
      p_sql        IN CLOB                 - The SQL statement to execute with binds or log.
      p_array_bind IN SYS.ODCIVARCHAR2LIST - List of bind variables for SQL execution.
      p_execute    IN VARCHAR2             - Controls execution.
                                             If 'Y' (default), executes the SQL statement.
                                             If 'N', logs the SQL without execution.

    Returns:
      NUMBER - Row count if executed, NULL if in read mode.

    Note:
      Raises an error if the bind variable count exceeds 5.
  */
  FUNCTION fn_run_sql_in_bind
  (
    p_log_id     IN NUMBER,
    p_sql        IN CLOB,
    p_array_bind IN SYS.ODCIVARCHAR2LIST,
    p_execute    IN VARCHAR2 DEFAULT 'Y'
  ) 
  RETURN NUMBER;  
END PKG_SQL;
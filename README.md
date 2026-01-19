# Oracle Tablespace Cleanup Project

## Overview

This project provides a comprehensive solution for Oracle database tablespace cleanup and rebuild operations. It consists of PL/SQL packages and supporting database objects that automate the process of moving database objects between tablespaces, fixing unusable indexes, and managing rebuild operations.

## Purpose

The system is designed to:
- Automate tablespace rebuild operations for database maintenance
- Move tables, indexes, LOBs, and partitions between tablespaces
- Fix unusable indexes and invalid objects
- Provide logging and error handling for all operations
- Support parallel execution for performance optimization

## Architecture

### Packages

#### PKG_SQL
- **Purpose**: Dynamic SQL execution with logging
- **Key Functions**:
  - `fn_run_sql()`: Executes DDL statements with error handling and logging

#### PKG_TL_LOGGING
- **Purpose**: Centralized logging for all operations
- **Key Procedures**:
  - `prc_log()`: Logs messages with status, error codes, and session information

#### PKG_TBS_REBUILD
- **Purpose**: Main package for tablespace rebuild operations
- **Key Functions**:
  - `fn_create_run_primary()`: Creates a primary rebuild run
  - `fn_create_run_fix_unusable()`: Creates a run to fix unusable indexes
  - `fn_create_run_fix_invalid()`: Creates a run to fix invalid objects
- **Key Procedures**:
  - `prc_process_run()`: Executes the rebuild steps
  - `prc_rollback_run()`: Rolls back executed operations
  - `prc_drop_empty_datafiles()`: Cleans up empty datafiles

### Database Objects

#### Tables
- **MD_PROCESS_LOG**: Partitioned logging table for all operations
- **TBS_REBUILD_RUN**: Master table for rebuild runs
- **TBS_REBUILD_RUN_STEPS**: Detail table for individual rebuild steps

#### Sequences
- **MD_PROCESS_LOG_SEQ**: Sequence for log IDs
- **TBS_REBUILD_RUN_S**: Sequence for run IDs
- **TBS_REBUILD_RUN_STEPS_S**: Sequence for step IDs

#### Views
- **VW_TABLE_REBUILD**: Provides DDL for moving tables and partitions
- **VW_INDEX_REBUILD**: Provides DDL for rebuilding indexes
- **VW_LOB_REBUILD**: Provides DDL for moving LOB segments
- **VW_ATTRIBUTE_REBUILD**: Provides DDL for modifying default attributes

## Usage

### Basic Workflow

1. **Create a rebuild run**:
   ```sql
   DECLARE
     l_run_id NUMBER;
   BEGIN
     l_run_id := pkg_tbs_rebuild.fn_create_run_primary(
       p_owner_filter      => 'SCOTT',
       p_source_tablespace => 'OLD_TBS',
       p_target_tablespace => 'NEW_TBS',
       p_parallel_degree   => 4
     );
   END;
   ```

2. **Execute the run**:
   ```sql
   BEGIN
     pkg_tbs_rebuild.prc_process_run(
       p_run_id        => l_run_id,
       p_execute       => 'Y',
       p_stop_on_error => 'N'
     );
   END;
   ```

3. **Monitor progress**:
   ```sql
   SELECT * FROM tbs_rebuild_run WHERE run_id = l_run_id;
   SELECT * FROM tbs_rebuild_run_steps WHERE run_id = l_run_id ORDER BY step_order;
   ```

### Views Usage

The views provide pre-generated DDL statements with placeholders:
- `<TABLESPACE>`: Replace with target tablespace name
- `<PARALLEL>`: Replace with parallel degree (e.g., `PARALLEL 4`)

Example:
```sql
SELECT move_ddl,
       REPLACE(REPLACE(move_ddl, '<TABLESPACE>', 'NEW_TBS'), '<PARALLEL>', 'PARALLEL 4') AS ready_ddl
FROM vw_table_rebuild
WHERE owner = 'SCOTT';
```

## Dependencies

- Oracle Database 19c or higher
- Access to DBA_* views
- EXECUTE privileges on DBMS_METADATA
- Appropriate tablespace and object privileges

## Logging

All operations are logged to the MD_PROCESS_LOG table with:
- Operation status (OK, ERROR, etc.)
- Execution times
- Error messages and codes
- Session information

## Error Handling

- Operations continue on individual step failures (configurable)
- Detailed error logging for troubleshooting
- Rollback capabilities for executed operations

## Security Considerations

- Packages should be granted appropriate privileges
- Logging captures session information for audit trails
- DDL execution requires proper database privileges

## Maintenance

- Regular cleanup of old log partitions
- Monitor sequence values for potential wraparound
- Review and optimize view queries for performance
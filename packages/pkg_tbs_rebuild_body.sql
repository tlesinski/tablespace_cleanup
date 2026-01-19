create or replace PACKAGE BODY pkg_tbs_rebuild IS

   ---------------------------------------------------------------------------
   -- PRIVATE CONSTANTS
   ---------------------------------------------------------------------------
   c_step_status_pending CONSTANT VARCHAR2(20) := 'PENDING';
   c_step_status_running CONSTANT VARCHAR2(20) := 'RUNNING';
   c_step_status_ok      CONSTANT VARCHAR2(20) := 'OK';
   c_step_status_error   CONSTANT VARCHAR2(20) := 'ERROR';
   c_step_status_skipped CONSTANT VARCHAR2(20) := 'SKIPPED';

   c_step_type_primary      CONSTANT VARCHAR2(30) := 'PRIMARY';
   c_step_type_fix_unusable CONSTANT VARCHAR2(30) := 'FIX_UNUSABLE';
   c_step_type_fix_invalid  CONSTANT VARCHAR2(30) := 'FIX_INVALID';

   c_yes CONSTANT VARCHAR2(1) := 'Y';
   c_no  CONSTANT VARCHAR2(1) := 'N';

   ---------------------------------------------------------------------------
   -- LOCAL HELPERS: ID generators
   ---------------------------------------------------------------------------
   FUNCTION fn_get_next_run_id
      RETURN NUMBER
   IS
      l_run_id NUMBER;
   BEGIN
      SELECT tbs_rebuild_run_s.NEXTVAL
        INTO l_run_id
        FROM dual;

      RETURN l_run_id;
   END fn_get_next_run_id;


   FUNCTION fn_get_next_step_id
      RETURN NUMBER
   IS
      l_step_id NUMBER;
   BEGIN
      SELECT tbs_rebuild_run_steps_s.NEXTVAL
        INTO l_step_id
        FROM dual;

      RETURN l_step_id;
   END fn_get_next_step_id;


   ---------------------------------------------------------------------------
   -- PRIVATE: insert run header
   ---------------------------------------------------------------------------
   PROCEDURE prc_insert_run_header (
      p_run_type          IN VARCHAR2,
      p_owner_filter      IN VARCHAR2,
      p_source_tablespace IN VARCHAR2,
      p_target_tablespace IN VARCHAR2,
      p_parallel_degree   IN PLS_INTEGER,
      p_run_id_out       OUT NUMBER
   )
   IS
   BEGIN
      p_run_id_out := fn_get_next_run_id;

      INSERT INTO tbs_rebuild_run 
      (
         run_id,
         run_type,
         start_time,
         status,
         owner_filter,
         source_tablespace,
         target_tablespace,
         parallel_degree,
         created_at,
         created_by
      )
      VALUES (
         p_run_id_out,
         p_run_type,
         NULL,
         g_status_new,
         p_owner_filter,
         p_source_tablespace,
         p_target_tablespace,
         p_parallel_degree,
         SYSDATE,
         USER
      );
   END prc_insert_run_header;


   ---------------------------------------------------------------------------
   -- PRIVATE: update totals in master
   ---------------------------------------------------------------------------
   PROCEDURE prc_update_run_totals (
      p_run_id IN NUMBER
   )
   IS
   BEGIN
      UPDATE tbs_rebuild_run r
         SET (total_steps, steps_ok, steps_error) =
             (SELECT COUNT(*),
                     SUM(CASE WHEN s.status = c_step_status_ok THEN 1 ELSE 0 END),
                     SUM(CASE WHEN s.status = c_step_status_error THEN 1 ELSE 0 END)
                FROM tbs_rebuild_run_steps s
               WHERE s.run_id = r.run_id)
       WHERE r.run_id = p_run_id;
   END prc_update_run_totals;


   ---------------------------------------------------------------------------
   -- PRIVATE: helper for inserting step
   ---------------------------------------------------------------------------
   PROCEDURE prc_insert_step 
   (
      p_run_id               IN NUMBER,
      p_step_order           IN NUMBER,
      p_owner_name           IN VARCHAR2,
      p_object_name          IN VARCHAR2,
      p_object_type          IN VARCHAR2,
      p_segment_type         IN VARCHAR2,
      p_partition_name       IN VARCHAR2,
      p_subpartition_name    IN VARCHAR2,
      p_operation_type       IN VARCHAR2,
      p_step_type            IN VARCHAR2,
      p_online_flag          IN CHAR,
      p_orig_parallel_degree IN VARCHAR2,
      p_ddl_text             IN CLOB,
      p_ddl_text_orig_par    IN CLOB,
      p_rollback_ddl_text    IN CLOB,
      p_ddl_generator_name   IN VARCHAR2
   )
   IS
      l_step_id NUMBER;
   BEGIN
      l_step_id := fn_get_next_step_id;

      INSERT INTO tbs_rebuild_run_steps 
      (
         step_id,
         run_id,
         step_order,
         owner_name,
         object_name,
         object_type,
         segment_type,
         partition_name,
         subpartition_name,
         operation_type,
         step_type,
         online_flag,
         orig_parallel_degree,
         ddl_text,
         ddl_text_orig_parallel,
         rollback_ddl_text,
         ddl_generator_name,
         status,
         attempts,
         created_at,
         created_by
      )
      VALUES 
      (
         l_step_id,
         p_run_id,
         p_step_order,
         p_owner_name,
         p_object_name,
         p_object_type,
         p_segment_type,
         p_partition_name,
         p_subpartition_name,
         p_operation_type,
         p_step_type,
         p_online_flag,
         p_orig_parallel_degree,
         p_ddl_text,
         p_ddl_text_orig_par,
         p_rollback_ddl_text,
         p_ddl_generator_name,
         c_step_status_pending,
         0,
         SYSDATE,
         USER
      );
   END prc_insert_step;


   ---------------------------------------------------------------------------
   -- PRIVATE: generators for PRIMARY RUN
   -- (na razie szkielety, do uzupełnienia przez Ciebie)
   ---------------------------------------------------------------------------
  PROCEDURE prc_gen_steps_for_tables
  (
     p_run_id            IN NUMBER,
     p_owner_filter      IN VARCHAR2,
     p_source_tablespace IN VARCHAR2,
     p_target_tablespace IN VARCHAR2,
     p_parallel_degree   IN PLS_INTEGER
  )
  IS
    l_proc_name varchar2(128) := 'prc_gen_steps_for_tables';
  BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_run_id:            ' || p_run_id               || CHR(10) ||
                   ' p_owner_filter:      ' || p_owner_filter         || chr(10) ||
                   ' p_source_tablespace: ' || p_source_tablespace    || chr(10) ||
                   ' p_target_tablespace: ' || p_target_tablespace    || chr(10) ||
                   ' p_parallel_degree:   ' || p_parallel_degree 
    ) ; 

    FOR i IN 
    (
      SELECT 
          owner, segment_name, segment_type, column_name, 
          partition_name, subpartition_name, tablespace_name, degree, 
          replace(replace(move_ddl, '<TABLESPACE>', p_target_tablespace), '<PARALLEL>',p_parallel_degree) move_ddl, 
          restore_parallel, 
          replace(replace(move_ddl, '<TABLESPACE>', tablespace_name), '<PARALLEL>',p_parallel_degree) move_restore_ddl 
        FROM vw_table_rebuild
       WHERE segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION')
         AND owner=nvl(p_owner_filter, owner)  
         AND tablespace_name = p_source_tablespace
    ) 
    LOOP
      prc_insert_step
      (
        p_run_id             => p_run_id,
        p_step_order         => tbs_rebuild_run_steps_s.nextval,
        p_owner_name         => i.owner,
        p_object_name        => i.segment_name,
        p_object_type        => i.segment_type,
        p_segment_type       => i.segment_type,
        p_partition_name     => i.partition_name,
        p_subpartition_name  => i.subpartition_name,
        p_operation_type     => 'MOVE',
        p_step_type          => g_primary_const,
        p_online_flag        => 'N',
        p_orig_parallel_degree => i.degree,
        p_ddl_text           => i.move_ddl,
        p_ddl_text_orig_par  => i.restore_parallel,
        p_rollback_ddl_text  => i.move_restore_ddl,
        p_ddl_generator_name => 'prc_gen_steps_for_tables'
      );
    END LOOP;
    
    COMMIT;
    
    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Completed ' || l_proc_name
    ) ; 
  END prc_gen_steps_for_tables;

  PROCEDURE prc_gen_steps_for_indexes 
  (
     p_run_id            IN NUMBER,
     p_owner_filter      IN VARCHAR2,
     p_source_tablespace IN VARCHAR2,
     p_target_tablespace IN VARCHAR2,
     p_parallel_degree   IN PLS_INTEGER
  )
  IS
    l_ddl_restore     clob;
    l_ddl             clob; 
    l_proc_name varchar2(128) := 'prc_gen_steps_for_indexes';
  BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_run_id:            ' || p_run_id               || CHR(10) ||
                   ' p_owner_filter:      ' || p_owner_filter         || chr(10) ||
                   ' p_source_tablespace: ' || p_source_tablespace    || chr(10) ||
                   ' p_target_tablespace: ' || p_target_tablespace    || chr(10) ||
                   ' p_parallel_degree:   ' || p_parallel_degree 
    ) ; 
    
    FOR i IN (
      SELECT 
          owner, segment_name, segment_type, column_name, 
          partition_name, subpartition_name, tablespace_name, degree, 
          replace(replace(move_ddl, '<TABLESPACE>', p_target_tablespace), '<PARALLEL>',p_parallel_degree) move_ddl, 
          restore_parallel, 
          replace(replace(move_ddl, '<TABLESPACE>', tablespace_name), '<PARALLEL>',p_parallel_degree) move_restore_ddl 
        FROM vw_index_rebuild
       WHERE segment_type IN ('INDEX','INDEX PARTITION','INDEX SUBPARTITION')
         AND owner=nvl(p_owner_filter, owner)  
         AND tablespace_name = p_source_tablespace
    ) 
    LOOP
      prc_insert_step
      (
        p_run_id             => p_run_id,
        p_step_order         => tbs_rebuild_run_steps_s.nextval,
        p_owner_name         => i.owner,
        p_object_name        => i.segment_name,
        p_object_type        => i.segment_type,
        p_segment_type       => i.segment_type,
        p_partition_name     => i.partition_name,
        p_subpartition_name  => i.subpartition_name,
        p_operation_type     => 'MOVE',
        p_step_type          => g_primary_const,
        p_online_flag          => 'N',
        p_orig_parallel_degree => i.degree,
        p_ddl_text             => i.move_ddl,
        p_ddl_text_orig_par    => i.restore_parallel,
        p_rollback_ddl_text    => i.move_restore_ddl,
        p_ddl_generator_name   => 'prc_gen_steps_for_indexes'
      );
    END LOOP;

    COMMIT;

    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Completed ' || l_proc_name
    ) ; 
  END prc_gen_steps_for_indexes;


  PROCEDURE prc_gen_steps_for_lobs 
  (
     p_run_id            IN NUMBER,
     p_owner_filter      IN VARCHAR2,
     p_source_tablespace IN VARCHAR2,
     p_target_tablespace IN VARCHAR2,
     p_parallel_degree   IN PLS_INTEGER
  )
  IS
    l_proc_name varchar2(128) := 'prc_gen_steps_for_lobs';
  BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_run_id:            ' || p_run_id               || CHR(10) ||
                   ' p_owner_filter:      ' || p_owner_filter         || chr(10) ||
                   ' p_source_tablespace: ' || p_source_tablespace    || chr(10) ||
                   ' p_target_tablespace: ' || p_target_tablespace    || chr(10) ||
                   ' p_parallel_degree:   ' || p_parallel_degree 
    ) ; 
    
    FOR i IN 
    (
      SELECT 
          owner, segment_name, segment_type, column_name, 
          partition_name, subpartition_name, tablespace_name, degree, 
          replace(replace(move_ddl, '<TABLESPACE>', p_target_tablespace), '<PARALLEL>',p_parallel_degree) move_ddl, 
          restore_parallel, 
          replace(replace(move_ddl, '<TABLESPACE>', tablespace_name), '<PARALLEL>',p_parallel_degree) move_restore_ddl 
        FROM vw_lob_rebuild
       WHERE segment_type IN ('LOB','LOB PARTITION','LOB SUBPARTITION')
         AND owner=nvl(p_owner_filter, owner)  
         AND tablespace_name = p_source_tablespace
    ) 
    LOOP
      prc_insert_step
      (
        p_run_id             => p_run_id,
        p_step_order         => tbs_rebuild_run_steps_s.nextval,
        p_owner_name         => i.owner,
        p_object_name        => i.segment_name,
        p_object_type        => i.segment_type,
        p_segment_type       => i.segment_type,
        p_partition_name     => i.partition_name,
        p_subpartition_name  => i.subpartition_name,
        p_operation_type     => 'MOVE',
        p_step_type          => g_primary_const,
        p_online_flag        => 'Y',
        p_orig_parallel_degree => i.degree,
        p_ddl_text             => i.move_ddl,
        p_ddl_text_orig_par    => i.restore_parallel,
        p_rollback_ddl_text    => i.move_restore_ddl,
        p_ddl_generator_name   => 'prc_gen_steps_for_lobs'
      );
    END LOOP;

    COMMIT;

    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Completed ' || l_proc_name
    ) ; 
  END prc_gen_steps_for_lobs;

  PROCEDURE prc_gen_steps_for_defaults 
  (
    p_run_id            IN NUMBER,
    p_owner_filter      IN VARCHAR2,
    p_source_tablespace IN VARCHAR2,
    p_target_tablespace IN VARCHAR2
  )
  IS
    l_proc_name varchar2(128) := 'prc_gen_steps_for_defaults';
  BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_run_id:            ' || p_run_id               || CHR(10) ||
                   ' p_owner_filter:      ' || p_owner_filter         || chr(10) ||
                   ' p_source_tablespace: ' || p_source_tablespace    || chr(10) ||
                   ' p_target_tablespace: ' || p_target_tablespace
    ) ; 
    
    FOR i IN 
    (
      SELECT 
          owner, segment_name, segment_type, column_name, 
          partition_name, subpartition_name, tablespace_name, degree, 
          replace(replace(move_ddl, '<TABLESPACE>', p_target_tablespace), '<PARALLEL>',1) move_ddl, 
          restore_parallel, 
          replace(replace(move_ddl, '<TABLESPACE>', tablespace_name), '<PARALLEL>',1) move_restore_ddl 
        FROM vw_attribute_rebuild
       WHERE segment_type IN ('TABLE ATTRIBUTES','INDEX ATTRIBUTES')
         AND owner=nvl(p_owner_filter, owner)  
         AND tablespace_name = p_source_tablespace
    ) 
    LOOP
      prc_insert_step
      (
        p_run_id             => p_run_id,
        p_step_order         => tbs_rebuild_run_steps_s.nextval,
        p_owner_name         => i.owner,
        p_object_name        => i.segment_name,
        p_object_type        => i.segment_type,
        p_segment_type       => i.segment_type,
        p_partition_name     => i.partition_name,
        p_subpartition_name  => i.subpartition_name,
        p_operation_type     => 'MOVE',
        p_step_type          => g_primary_const,
        p_online_flag        => 'N',
        p_orig_parallel_degree => NULL,
        p_ddl_text           => i.move_ddl,
        p_ddl_text_orig_par  => i.restore_parallel,
        p_rollback_ddl_text  => i.move_restore_ddl,
        p_ddl_generator_name => 'prc_gen_steps_for_defaults'
      );
    END LOOP;

    COMMIT;

    pkg_tl_logging.prc_log 
    (  
      p_log_id => p_run_id,
      p_log_msg => 'Completed ' || l_proc_name
    ) ; 
  END prc_gen_steps_for_defaults;

   ---------------------------------------------------------------------------
   -- PRIVATE: FIX_UNUSABLE generator (szkielet)
   ---------------------------------------------------------------------------
   PROCEDURE prc_gen_steps_for_unusable_indexes (
      p_run_id        IN NUMBER,
      p_owner_filter  IN VARCHAR2,
      p_parallel_degree IN PLS_INTEGER
   )
   IS
   BEGIN
      NULL;
      -- TODO: SELECT z dba_indexes/dba_ind_partitions/dba_ind_subpartitions
      --  WHERE status = 'UNUSABLE'
      --  i prc_insert_step(..., step_type => c_step_type_fix_unusable, ...)
   END prc_gen_steps_for_unusable_indexes;


   ---------------------------------------------------------------------------
   -- PRIVATE: FIX_INVALID generator (szkielet)
   ---------------------------------------------------------------------------
   PROCEDURE prc_gen_steps_for_invalid_objects (
      p_run_id       IN NUMBER,
      p_owner_filter IN VARCHAR2
   )
   IS
   BEGIN
      NULL;
      -- TODO: SELECT z dba_objects WHERE status='INVALID'
      -- i generacja ALTER ... COMPILE jako kroków FIX_INVALID
   END prc_gen_steps_for_invalid_objects;


   ---------------------------------------------------------------------------
   -- PUBLIC: fn_create_run_primary
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_primary 
   (
      p_owner_filter      IN VARCHAR2  DEFAULT NULL,
      p_source_tablespace IN VARCHAR2,
      p_target_tablespace IN VARCHAR2,
      p_parallel_degree   IN PLS_INTEGER DEFAULT 4
   ) 
   RETURN NUMBER
   IS
     l_run_id NUMBER := tbs_rebuild_run_s.NEXTVAL;
     l_cnt    NUMBER; 
     l_proc_name VARCHAR2(128) := 'fn_create_run_primary';
   BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => l_run_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_owner_filter:      ' || p_owner_filter         || chr(10) ||
                   ' p_source_tablespace: ' || p_source_tablespace    || chr(10) ||
                   ' p_target_tablespace: ' || p_target_tablespace    || chr(10) ||
                   ' p_parallel_degree:   ' || p_parallel_degree ,
      p_log_categ => 'CREATE_PRIMARY',
      p_log_sttus => pkg_tl_logging.g_sttus_running_const,
      p_start_date => sysdate
    ) ; 
     
     SELECT COUNT(*) 
       INTO l_cnt 
       FROM dba_tablespaces
      WHERE tablespace_name IN (p_source_tablespace, p_target_tablespace);
     
      IF l_cnt != 2 THEN
        raise_application_error(-20001, 'Tablespace ' || p_source_tablespace || ' or ' || p_target_tablespace ||' don''t exist');
      END IF;
      
      IF NOT(p_parallel_degree >= 1) THEN
        raise_application_error(-20001, 'Parameter p_parallel_degree must be >= 1');
      END IF;
      
      INSERT INTO tbs_rebuild_run (
         run_id,
         run_type,
         start_time,
         status,
         owner_filter,
         source_tablespace,
         target_tablespace,
         parallel_degree,
         created_at,
         created_by
      )
      VALUES (
         l_run_id,
         'PRIMARY',
         NULL,
         g_status_new,
         p_owner_filter,
         p_source_tablespace,
         p_target_tablespace,
         p_parallel_degree,
         SYSDATE,
         USER
      );

    COMMIT;

      -- Generacja kroków
      prc_gen_steps_for_tables
      (
         p_run_id            => l_run_id,
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => p_source_tablespace,
         p_target_tablespace => p_target_tablespace,
         p_parallel_degree   => p_parallel_degree
      );

      prc_gen_steps_for_indexes
      (
         p_run_id            => l_run_id,
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => p_source_tablespace,
         p_target_tablespace => p_target_tablespace,
         p_parallel_degree   => p_parallel_degree
      );

      prc_gen_steps_for_lobs
      (
         p_run_id            => l_run_id,
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => p_source_tablespace,
         p_target_tablespace => p_target_tablespace,
         p_parallel_degree   => p_parallel_degree
      );

      prc_gen_steps_for_defaults
      (
         p_run_id            => l_run_id,
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => p_source_tablespace,
         p_target_tablespace => p_target_tablespace
      );

      UPDATE tbs_rebuild_run
         SET status = g_status_planned
       WHERE run_id = l_run_id;

      prc_update_run_totals(l_run_id);
    
    COMMIT;
    
    pkg_tl_logging.prc_log 
    (  
      p_log_id    => l_run_id,
      p_log_msg   => 'Completed',
      p_log_sttus => pkg_tl_logging.g_sttus_success_const,
      p_end_date  => sysdate
    ) ; 

    RETURN l_run_id;
  EXCEPTION
    WHEN OTHERS THEN
      pkg_tl_logging.prc_error_stack
      (
        p_log_id => l_run_id
      );
  
   RETURN 0;
  END fn_create_run_primary;


   ---------------------------------------------------------------------------
   -- PUBLIC: fn_create_run_fix_unusable
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_fix_unusable (
      p_owner_filter     IN VARCHAR2  DEFAULT NULL,
      p_parallel_degree  IN PLS_INTEGER DEFAULT 4
   ) RETURN NUMBER
   IS
      l_run_id NUMBER;
   BEGIN
      prc_insert_run_header(
         p_run_type          => 'FIX_UNUSABLE',
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => NULL,
         p_target_tablespace => NULL,
         p_parallel_degree   => p_parallel_degree,
         p_run_id_out        => l_run_id
      );

      prc_gen_steps_for_unusable_indexes(
         p_run_id          => l_run_id,
         p_owner_filter    => p_owner_filter,
         p_parallel_degree => p_parallel_degree
      );

      UPDATE tbs_rebuild_run
         SET status = g_status_planned
       WHERE run_id = l_run_id;

      prc_update_run_totals(l_run_id);

      RETURN l_run_id;
   END fn_create_run_fix_unusable;


   ---------------------------------------------------------------------------
   -- PUBLIC: fn_create_run_fix_invalid
   ---------------------------------------------------------------------------
   FUNCTION fn_create_run_fix_invalid (
      p_owner_filter     IN VARCHAR2  DEFAULT NULL
   ) RETURN NUMBER
   IS
      l_run_id NUMBER;
   BEGIN
      prc_insert_run_header(
         p_run_type          => 'FIX_INVALID',
         p_owner_filter      => p_owner_filter,
         p_source_tablespace => NULL,
         p_target_tablespace => NULL,
         p_parallel_degree   => NULL,
         p_run_id_out        => l_run_id
      );

      prc_gen_steps_for_invalid_objects(
         p_run_id       => l_run_id,
         p_owner_filter => p_owner_filter
      );

      UPDATE tbs_rebuild_run
         SET status = g_status_planned
       WHERE run_id = l_run_id;

      prc_update_run_totals(l_run_id);

      RETURN l_run_id;
   END fn_create_run_fix_invalid;


  ---------------------------------------------------------------------------
  -- PUBLIC: prc_process_run
  ---------------------------------------------------------------------------
  PROCEDURE prc_process_run 
  (
    p_run_id        IN NUMBER,
    p_execute       IN VARCHAR2 DEFAULT 'Y',
    p_stop_on_error IN VARCHAR2 DEFAULT 'N',
    p_parallel_exec IN PLS_INTEGER DEFAULT NULL
  )
  IS
    l_stop_on_err   BOOLEAN := (p_stop_on_error = c_yes);
    l_err_msg       VARCHAR2(4000);
    l_status        VARCHAR2(20);
    l_now           DATE;
    l_sql           CLOB;
    l_proc_name     VARCHAR2(128) := 'prc_process_run';
    l_log_id        NUMBER := tbs_rebuild_run_s.NEXTVAL;
    l_rows          NUMBER;
    l_error         NUMBER := 0;     
  BEGIN
    pkg_tl_logging.prc_log 
    (  
      p_log_id => l_log_id,
      p_log_msg => 'Starting ' || l_proc_name || ' with parameters: ' || chr(10) ||
                   ' p_run_id:        ' || p_run_id         || chr(10) ||
                   ' p_execute:       ' || p_execute        || chr(10) ||
                   ' p_stop_on_error: ' || p_stop_on_error  || chr(10) ||
                   ' p_parallel_exec: ' || p_parallel_exec,
      p_log_categ  => 'RUN_PRIMARY',
      p_log_sttus  => pkg_tl_logging.g_sttus_running_const,
      p_start_date => sysdate
    ) ; 
    
    IF p_execute NOT IN ('Y', 'N') THEN
      raise_application_error(-20001, 'p_execute NOT IN (''Y'', ''N'')');
    END IF;
    
    UPDATE tbs_rebuild_run
       SET status     = g_status_running,
           start_time = NVL(start_time, SYSDATE)
     WHERE run_id = p_run_id;

    COMMIT;
    
    FOR i IN 
    (
      SELECT 
          r.SOURCE_TABLESPACE,
          r.TARGET_TABLESPACE,
          r.PARALLEL_DEGREE,
          s.STEP_ID,
          s.RUN_ID,
          s.STEP_ORDER,
          s.OWNER_NAME,
          s.OBJECT_NAME,
          s.OBJECT_TYPE,
          s.SEGMENT_TYPE,
          s.PARTITION_NAME,
          s.SUBPARTITION_NAME,
          s.OPERATION_TYPE,
          s.STEP_TYPE,
          s.ONLINE_FLAG,
          s.orig_PARALLEL_DEGREE,
          s.DDL_TEXT,
          s.DDL_TEXT_ORIG_PARALLEL,
          s.ROLLBACK_DDL_TEXT
        FROM tbs_rebuild_run r,tbs_rebuild_run_steps s
       WHERE r.run_id = p_run_id
         and r.run_id = s.run_id
         AND s.status IN (c_step_status_pending, c_step_status_error)
       ORDER BY s.step_order
    )
    LOOP
      l_err_msg := NULL;
      l_status  := c_step_status_ok;
      l_now     := SYSDATE;

      pkg_tl_logging.prc_log 
      (  
        p_log_id => l_log_id,
        p_log_msg => 'Start step ' || i.step_id
      );
    
      UPDATE tbs_rebuild_run_steps
         SET status          = c_step_status_running,
             attempts        = attempts + 1,
             step_start_time = l_now
       WHERE step_id = i.step_id;
       
      COMMIT;
       
      BEGIN
        l_rows := pkg_sql.fn_run_sql
        ( 
          p_log_id  => l_log_id,
          p_sql     => i.ddl_text,
          p_execute => p_execute
        );
        
        IF i.ddl_text_orig_parallel IS NOT NULL THEN
          l_rows := pkg_sql.fn_run_sql
          ( 
            p_log_id  => l_log_id,
            p_sql     => i.ddl_text_orig_parallel,
            p_execute => p_execute
          );
         END IF;
      EXCEPTION
        WHEN OTHERS THEN
           l_err_msg := SQLERRM;
           l_status  := c_step_status_error;
      END;

      l_now := SYSDATE;

      UPDATE tbs_rebuild_run_steps
         SET status        = l_status,
             error_message = l_err_msg,
             step_end_time = l_now,
             executed_at   = l_now
       WHERE step_id = i.step_id;

      IF l_status = c_step_status_error AND l_stop_on_err THEN
        l_error := 1;     
        EXIT;
      END IF;
    END LOOP;

    prc_update_run_totals(p_run_id);

    UPDATE tbs_rebuild_run
       SET end_time = SYSDATE,
           status   = DECODE(l_error, 0, pkg_tl_logging.g_sttus_success_const, pkg_tl_logging.g_sttus_error_const)
     WHERE run_id = p_run_id;
    
    COMMIT;
    
    pkg_tl_logging.prc_log 
    (  
      p_log_id    => l_log_id,
      p_log_msg   => 'Completed',
      p_log_sttus => CASE WHEN l_error = 0 THEN pkg_tl_logging.g_sttus_success_const ELSE pkg_tl_logging.g_sttus_error_const END,
      p_end_date  => sysdate
    ) ; 
  EXCEPTION
    WHEN OTHERS THEN
      pkg_tl_logging.prc_error_stack
      (
        p_log_id => l_log_id
      );
  END prc_process_run;


   ---------------------------------------------------------------------------
   -- PUBLIC: prc_rollback_run
   ---------------------------------------------------------------------------
   PROCEDURE prc_rollback_run 
   (
      p_run_id        IN NUMBER,
      p_execute       IN VARCHAR2 DEFAULT 'Y',
      p_stop_on_error IN VARCHAR2 DEFAULT 'N'
   )
   IS
      CURSOR c_steps IS
         SELECT *
           FROM tbs_rebuild_run_steps
          WHERE run_id = p_run_id
          ORDER BY step_order DESC;

      l_do_execute  BOOLEAN := (p_execute = c_yes);
      l_stop_on_err BOOLEAN := (p_stop_on_error = c_yes);
      l_err_msg     VARCHAR2(4000);
      l_status      VARCHAR2(20);
      l_now         DATE;
   BEGIN
      FOR r IN c_steps LOOP
         EXIT WHEN r.rollback_ddl_text IS NULL; -- nic do rollbacku

         l_err_msg := NULL;
         l_status  := c_step_status_ok;
         l_now     := SYSDATE;

         BEGIN
            IF l_do_execute THEN
               EXECUTE IMMEDIATE r.rollback_ddl_text;
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               l_err_msg := SQLERRM;
               l_status  := c_step_status_error;
         END;

         UPDATE tbs_rebuild_run_steps
            SET status        = l_status,
                error_message = NVL(error_message, l_err_msg),
                step_end_time = SYSDATE,
                executed_at   = SYSDATE
          WHERE step_id = r.step_id;

         IF l_status = c_step_status_error AND l_stop_on_err THEN
            EXIT;
         END IF;
      END LOOP;

      prc_update_run_totals(p_run_id);
   END prc_rollback_run;


   ---------------------------------------------------------------------------
   -- PUBLIC: prc_drop_empty_datafiles (szkielet)
   ---------------------------------------------------------------------------
   PROCEDURE prc_drop_empty_datafiles (
      p_tablespace_name IN VARCHAR2,
      p_execute         IN VARCHAR2 DEFAULT 'N'
   )
   IS
      l_do_execute  BOOLEAN := (p_execute = c_yes);
      l_cnt_files   PLS_INTEGER;
      l_file_id     NUMBER;
      l_file_name   VARCHAR2(513);
      l_has_extents PLS_INTEGER;
   BEGIN
      -- policz file count
      SELECT COUNT(*)
        INTO l_cnt_files
        FROM dba_data_files
       WHERE tablespace_name = UPPER(p_tablespace_name);

      IF l_cnt_files <= 1 THEN
         -- nie ruszamy TS z jednym plikiem
         RETURN;
      END IF;

      FOR r IN (
         SELECT file_id, file_name
           FROM dba_data_files
          WHERE tablespace_name = UPPER(p_tablespace_name)
      ) LOOP
         l_file_id   := r.file_id;
         l_file_name := r.file_name;

         -- sprawdź, czy są extents w tym pliku
         SELECT COUNT(*)
           INTO l_has_extents
           FROM dba_extents
          WHERE file_id = l_file_id;

         IF l_has_extents = 0 THEN
            -- mamy kandydata do DROP DATAFILE
            IF l_do_execute THEN
               BEGIN
                  EXECUTE IMMEDIATE
                        'ALTER TABLESPACE '
                     || DBMS_ASSERT.simple_sql_name(UPPER(p_tablespace_name))
                     || ' DROP DATAFILE ''' || l_file_name || '''';
               EXCEPTION
                  WHEN OTHERS THEN
                     -- tutaj można podłączyć logger
                     NULL;
               END;
            ELSE
               -- tryb dry-run -> można wypisać do DBMS_OUTPUT lub loggera
               DBMS_OUTPUT.put_line(
                  'Candidate empty datafile in TS '
                  || p_tablespace_name
                  || ': '
                  || l_file_name
               );
            END IF;
         END IF;
      END LOOP;
   END prc_drop_empty_datafiles;


END pkg_tbs_rebuild;
/
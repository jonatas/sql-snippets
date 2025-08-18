-- Partial refresh system to allow to refresh aggregates based on a specific column.
-- Problem: Continuous Aggregates does not allow to have partial refreshes for custom backfills.

-- Solution: Build a flexible invalidation logs that allows to track changes and selectively refresh aggregates.
-- Usage:
-- SELECT enable_partial_refresh('cagg_name', 'column_name', 'minimal_time_window');
-- Instead of using refresh_continuous_aggregates, now you can refresh partial aggregates automatically
-- 
-- Schedule automatic processing:
-- SELECT schedule_invalidation_job('30 seconds', 10);
--
-- Manual processing:
-- CALL process_flexible_invalidation_job(0, '{"batch_limit": 10}');
DROP TABLE IF EXISTS metrics CASCADE;
-- Create the metrics hypertable
CREATE TABLE IF NOT EXISTS metrics (
    time TIMESTAMPTZ NOT NULL,
    tag_id INTEGER,
    value DOUBLE PRECISION,
    device_id TEXT
);

-- Convert to hypertable
SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

-- Create a continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT time_bucket('1 hour', time) AS bucket,
       tag_id,
       COUNT(*) as count,
       AVG(value) as avg_value,
       MIN(value) as min_value,
       MAX(value) as max_value
FROM metrics
GROUP BY bucket, tag_id
WITH NO DATA;

-- Add sample data for testing
INSERT INTO metrics (time, tag_id, value, device_id) VALUES 
    (NOW() - INTERVAL '3 hours', 100, 10.5, 'device_1'),
    (NOW() - INTERVAL '2 days', 100, 15.2, 'device_1'),
    (NOW() - INTERVAL '1 hour', 200, 8.7, 'device_2'),
    (NOW() - INTERVAL '30 minutes', 200, 12.1, 'device_2'),
    (NOW() - INTERVAL '10 minutes', 300, 5.9, 'device_3')
ON CONFLICT DO NOTHING;

-- Create custom types
DROP TYPE IF EXISTS invalidation_status CASCADE;
CREATE TYPE invalidation_status AS ENUM ('pending', 'processing', 'completed', 'failed');

-- Create a function to generate upsert query templates from metadata
CREATE OR REPLACE FUNCTION generate_upsert_template(
    p_cagg_name TEXT,
    p_partition_column TEXT
) RETURNS TEXT AS $$
DECLARE
    v_view_schema TEXT;
    v_view_name TEXT;
    v_view_definition TEXT;
    v_mat_hypertable_schema TEXT;
    v_mat_hypertable_name TEXT;
    v_time_column TEXT;
    v_columns TEXT[];
    v_grouping_columns TEXT[];
    v_aggregate_columns TEXT[];
    v_template TEXT;
    v_col_record RECORD;
    v_select_part TEXT;
    v_from_table TEXT;
    v_group_by_part TEXT;
BEGIN
    -- Get continuous aggregate metadata
    SELECT 
        ca.view_schema,
        ca.view_name,
        ca.view_definition,
        ca.materialization_hypertable_schema,
        ca.materialization_hypertable_name
    INTO 
        v_view_schema,
        v_view_name,
        v_view_definition,
        v_mat_hypertable_schema,
        v_mat_hypertable_name
    FROM timescaledb_information.continuous_aggregates ca
    WHERE ca.view_name = p_cagg_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Continuous aggregate % not found', p_cagg_name;
    END IF;
    
    -- Get time dimension info  
    SELECT d.column_name
    INTO v_time_column
    FROM timescaledb_information.dimensions d
    WHERE d.hypertable_schema = v_mat_hypertable_schema
      AND d.hypertable_name = v_mat_hypertable_name
      AND d.dimension_type = 'Time'
    ORDER BY d.dimension_number
    LIMIT 1;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Time dimension not found for materialization hypertable %.%', 
                        v_mat_hypertable_schema, v_mat_hypertable_name;
    END IF;
    
    -- Get all columns from the materialized hypertable
    FOR v_col_record IN 
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = v_mat_hypertable_schema
          AND table_name = v_mat_hypertable_name
          AND column_name NOT IN ('chunk_id', 'compressed_chunk_id')
        ORDER BY ordinal_position
    LOOP
        v_columns := v_columns || v_col_record.column_name;
        
        -- Categorize columns: time column and partition column are grouping columns
        -- Everything else is likely an aggregate column
        IF v_col_record.column_name = v_time_column OR v_col_record.column_name = p_partition_column THEN
            v_grouping_columns := v_grouping_columns || v_col_record.column_name;
        ELSE
            v_aggregate_columns := v_aggregate_columns || v_col_record.column_name;
        END IF;
    END LOOP;
    
    -- Build the MERGE query template with placeholders
    -- Extract and modify the CAGG view definition for time range filtering
    -- Remove the trailing semicolon and extract the core SELECT statement
    
    -- Extract table name from FROM clause
    v_from_table := (regexp_matches(v_view_definition, 'FROM\s+(\w+)', 'i'))[1];
    
    -- Extract SELECT clause (everything before FROM)
    v_select_part := regexp_replace(v_view_definition, '\s*FROM.*$', '', 'i');
    
    -- Extract GROUP BY clause
    v_group_by_part := (regexp_matches(v_view_definition, 'GROUP BY\s+(.+?)(?:;|\s*$)', 'i'))[1];
    
    v_template := format('
MERGE INTO %I.%I AS target
USING (
    %s
    FROM %s
    WHERE time >= $1 AND time < $2
    %s
    GROUP BY %s
) AS source ON (%s)
WHEN MATCHED THEN 
    UPDATE SET %s
WHEN NOT MATCHED THEN
    INSERT (%s) VALUES (%s)',
        v_mat_hypertable_schema, v_mat_hypertable_name,
        v_select_part,
        v_from_table,
        CASE WHEN p_partition_column IS NOT NULL 
             THEN format('AND %I = ($3->>%L)::%s', 
                        p_partition_column, 
                        p_partition_column,
                        'int')
             ELSE '' 
        END,
        v_group_by_part,
        array_to_string(
            array(SELECT format('target.%I IS NOT DISTINCT FROM source.%I', col, col) 
                  FROM unnest(v_grouping_columns) AS col), 
            ' AND '
        ),
        array_to_string(
            array(SELECT format('%I = source.%I', col, col) 
                  FROM unnest(v_aggregate_columns) AS col), 
            ', '
        ),
        array_to_string(v_columns, ', '),
        array_to_string(
            array(SELECT format('source.%I', col) 
                  FROM unnest(v_columns) AS col), 
            ', '
        )
    );
    
    RETURN v_template;
END;
$$ LANGUAGE plpgsql;

-- Configuration tracking table
CREATE TABLE IF NOT EXISTS cagg_configurations (
    id SERIAL PRIMARY KEY,
    cagg_name TEXT UNIQUE NOT NULL,
    partition_column TEXT NOT NULL,
    minimal_window INTERVAL NOT NULL,
    upsert_query_template TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Function to populate upsert template automatically
CREATE OR REPLACE FUNCTION populate_upsert_template() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.upsert_query_template := generate_upsert_template(NEW.cagg_name, NEW.partition_column);
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-populate the upsert query template
DROP TRIGGER IF EXISTS populate_upsert_template_trigger ON cagg_configurations;
CREATE TRIGGER populate_upsert_template_trigger
    BEFORE INSERT OR UPDATE OF cagg_name, partition_column ON cagg_configurations
    FOR EACH ROW
    EXECUTE FUNCTION populate_upsert_template();

-- Flexible invalidation log table (uses JSONB for partition values)
-- Drop and recreate the table to ensure proper structure after type CASCADE
DROP TABLE IF EXISTS flexible_invalidation_log CASCADE;
CREATE TABLE flexible_invalidation_log (
    id SERIAL PRIMARY KEY,
    cagg_name TEXT NOT NULL,
    partition_values JSONB NOT NULL,
    partition_values_text TEXT GENERATED ALWAYS AS (partition_values::text) STORED,
    time_range TSTZRANGE NOT NULL,
    status invalidation_status DEFAULT 'pending',
    batch_id UUID DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE(cagg_name, partition_values_text, time_range)
);

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_flexible_invalidation_log_status ON flexible_invalidation_log(status);
CREATE INDEX IF NOT EXISTS idx_flexible_invalidation_log_cagg ON flexible_invalidation_log(cagg_name);
CREATE INDEX IF NOT EXISTS idx_flexible_invalidation_log_batch ON flexible_invalidation_log(batch_id);


-- Reusable function to create invalidation entries from a dataset
CREATE OR REPLACE FUNCTION create_invalidation_entries(
    p_cagg_name TEXT,
    p_partition_column TEXT,
    p_table_alias TEXT,
    p_minimal_window INTERVAL,
    p_batch_id UUID
) RETURNS TEXT AS $$
BEGIN
    RETURN format($sql$
        INSERT INTO flexible_invalidation_log (cagg_name, partition_values, time_range, batch_id)
        SELECT 
            '%s',
            jsonb_build_object('%s', grouped_data.%I),
            tstzrange(
                date_trunc('%s', grouped_data.min_time),
                date_trunc('%s', grouped_data.max_time) + '%s'::INTERVAL,
                '[)'
            ),
            '%s'
        FROM (
            SELECT 
                %s.%I,
                MIN(%s.time) as min_time,
                MAX(%s.time) as max_time
            FROM %s
            GROUP BY %s.%I
        ) grouped_data
        ON CONFLICT (cagg_name, partition_values_text, time_range)
        DO UPDATE SET 
            status = 'pending',
            batch_id = EXCLUDED.batch_id,
            created_at = NOW(),
            processed_at = NULL,
            error_message = NULL
    $sql$,
        p_cagg_name,
        p_partition_column,
        p_partition_column,
        CASE 
            WHEN p_minimal_window = '1 hour'::INTERVAL THEN 'hour'
            WHEN p_minimal_window = '1 day'::INTERVAL THEN 'day'
            ELSE 'hour'
        END,
        CASE 
            WHEN p_minimal_window = '1 hour'::INTERVAL THEN 'hour'
            WHEN p_minimal_window = '1 day'::INTERVAL THEN 'day'
            ELSE 'hour'
        END,
        p_minimal_window::TEXT,
        p_batch_id,
        p_table_alias,
        p_partition_column,
        p_table_alias,
        p_table_alias,
        p_table_alias,
        p_table_alias,
        p_partition_column
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced function to enable partial refresh with INSERT/UPDATE/DELETE support
CREATE OR REPLACE FUNCTION enable_partial_refresh(
    p_cagg_name TEXT,
    p_partition_column TEXT,
    p_minimal_window INTERVAL DEFAULT '1 hour'::INTERVAL,
    p_operations TEXT[] DEFAULT ARRAY['INSERT', 'UPDATE', 'DELETE']
) RETURNS TEXT AS $$
DECLARE
    trigger_name TEXT;
    trigger_sql TEXT;
    source_table TEXT;
    operation TEXT;
    result_messages TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Store configuration
    INSERT INTO cagg_configurations (cagg_name, partition_column, minimal_window)
    VALUES (p_cagg_name, p_partition_column, p_minimal_window)
    ON CONFLICT (cagg_name) DO UPDATE SET
        partition_column = EXCLUDED.partition_column,
        minimal_window = EXCLUDED.minimal_window,
        updated_at = NOW();

    -- Source table (hardcoded for this demo)
    SELECT hypertable_schema || '.' || hypertable_name INTO source_table
    FROM timescaledb_information.continuous_aggregates
    WHERE view_name = p_cagg_name;

    -- Create triggers for each requested operation
    FOREACH operation IN ARRAY p_operations LOOP
        trigger_name := p_cagg_name || '_invalidation_' || lower(operation);
        
        CASE operation
            WHEN 'INSERT' THEN
                trigger_sql := format($trigger$
                    CREATE OR REPLACE FUNCTION %I() RETURNS TRIGGER AS $func$
                    DECLARE
                        batch_uuid UUID := gen_random_uuid();
                        invalidation_sql TEXT;
                    BEGIN
                        -- Process inserted rows using reusable function
                        SELECT create_invalidation_entries('%s', '%s', 'inserted_rows', '%s'::INTERVAL, batch_uuid)
                        INTO invalidation_sql;
                        
                        EXECUTE invalidation_sql;
                        -- RAISE NOTICE 'Invalidation SQL: %%', invalidation_sql;
                        RETURN NULL;
                    END;
                    $func$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS %I ON %s;
                    CREATE TRIGGER %I
                        AFTER INSERT ON %s
                        REFERENCING NEW TABLE AS inserted_rows
                        FOR EACH STATEMENT
                        EXECUTE FUNCTION %I();
                $trigger$,
                    trigger_name || '_func',  -- Function name
                    p_cagg_name, p_partition_column, p_minimal_window::TEXT,  -- Function parameters
                    trigger_name, source_table,  -- DROP trigger
                    trigger_name, source_table,  -- CREATE trigger
                    trigger_name || '_func'  -- EXECUTE function
                );

            WHEN 'UPDATE' THEN
                trigger_sql := format($trigger$
                    CREATE OR REPLACE FUNCTION %I() RETURNS TRIGGER AS $func$
                    DECLARE
                        batch_uuid UUID := gen_random_uuid();
                        invalidation_sql TEXT;
                    BEGIN
                        -- Process old values (before update)
                        SELECT create_invalidation_entries('%s', '%s', 'old_rows', '%s'::INTERVAL, batch_uuid)
                        INTO invalidation_sql;
                        EXECUTE invalidation_sql;
                        
                        -- Process new values (after update)
                        SELECT create_invalidation_entries('%s', '%s', 'new_rows', '%s'::INTERVAL, batch_uuid)
                        INTO invalidation_sql;

                        EXECUTE invalidation_sql;
                        -- RAISE NOTICE 'Invalidation SQL: %%', invalidation_sql;
                        
                        RETURN NULL;
                    END;
                    $func$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS %I ON %s;
                    CREATE TRIGGER %I
                        AFTER UPDATE ON %s
                        REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
                        FOR EACH STATEMENT
                        EXECUTE FUNCTION %I();
                $trigger$,
                    trigger_name || '_func',  -- Function name
                    p_cagg_name, p_partition_column, p_minimal_window::TEXT,  -- First call params
                    p_cagg_name, p_partition_column, p_minimal_window::TEXT,  -- Second call params
                    trigger_name, source_table,  -- DROP trigger
                    trigger_name, source_table,  -- CREATE trigger
                    trigger_name || '_func'  -- EXECUTE function
                );

            WHEN 'DELETE' THEN
                trigger_sql := format($trigger$
                    CREATE OR REPLACE FUNCTION %I() RETURNS TRIGGER AS $func$
                    DECLARE
                        batch_uuid UUID := gen_random_uuid();
                        invalidation_sql TEXT;
                    BEGIN
                        -- Process deleted rows
                        SELECT create_invalidation_entries('%s', '%s', 'deleted_rows', '%s'::INTERVAL, batch_uuid)
                        INTO invalidation_sql;

                        EXECUTE invalidation_sql;
                        -- RAISE NOTICE 'Invalidation SQL: %%', invalidation_sql;
                        RETURN NULL;
                    END;
                    $func$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS %I ON %s;
                    CREATE TRIGGER %I
                        AFTER DELETE ON %s
                        REFERENCING OLD TABLE AS deleted_rows
                        FOR EACH STATEMENT
                        EXECUTE FUNCTION %I();
                $trigger$,
                    trigger_name || '_func',  -- Function name
                    p_cagg_name, p_partition_column, p_minimal_window::TEXT,  -- Function parameters
                    trigger_name, source_table,  -- DROP trigger
                    trigger_name, source_table,  -- CREATE trigger
                    trigger_name || '_func'  -- EXECUTE function
                );
        END CASE;

        EXECUTE trigger_sql;
        result_messages := array_append(result_messages, operation || ' trigger created');
    END LOOP;

    RETURN format('Partial refresh enabled for %s on column %s with %s windows. Operations: %s', 
                  p_cagg_name, p_partition_column, p_minimal_window, array_to_string(result_messages, ', '));
END;
$$ LANGUAGE plpgsql;

-- Background job processor - Using PROCEDURE to allow transaction control
-- Modified to work as a TimescaleDB scheduled job
CREATE OR REPLACE PROCEDURE process_flexible_invalidation_job(job_id int, config jsonb) AS $$
DECLARE
    log_rec RECORD;
    config_rec RECORD;
    processed_count INTEGER := 0;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    current_error TEXT;
    batch_limit INTEGER;
    max_age INTERVAL;
BEGIN
    -- Extract configuration parameters with defaults
    batch_limit := COALESCE((config->>'batch_limit')::INTEGER, 10);
    max_age := COALESCE((config->>'max_age')::INTERVAL, '1 hour'::INTERVAL);
    
    -- RAISE NOTICE 'Processing invalidation job (job_id: %, batch_limit: %, max_age: %)', 
                 job_id, batch_limit, max_age;
    FOR log_rec IN 
        SELECT * FROM flexible_invalidation_log 
        WHERE status = 'pending'
          AND (config->>'cagg_filter' IS NULL OR cagg_name = (config->>'cagg_filter'))
        ORDER BY created_at
        LIMIT batch_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Reset error for each iteration
        current_error := NULL;
        
        -- Get CAGG configuration
        SELECT * INTO config_rec FROM cagg_configurations 
        WHERE cagg_name = log_rec.cagg_name;
        
        IF NOT FOUND THEN
            current_error := 'No configuration found';
        ELSE
            -- Mark as processing
            UPDATE flexible_invalidation_log 
            SET status = 'processing', processed_at = NOW()
            WHERE id = log_rec.id;

            -- Try to execute the refresh
            start_time := lower(log_rec.time_range);
            end_time := upper(log_rec.time_range);
            
            -- Use our custom upsert function instead of TimescaleDB's refresh
            PERFORM execute_upsert_for_cagg(
                log_rec.cagg_name,
                start_time,
                end_time,
                log_rec.partition_values
            );
            
            processed_count := processed_count + 1;
        END IF;

        -- Update status based on whether there was an error
        IF current_error IS NOT NULL THEN
            UPDATE flexible_invalidation_log 
            SET status = 'failed',
                error_message = current_error,
                processed_at = NOW()
            WHERE id = log_rec.id;
        ELSE
            -- TODO: For production, consider implementing a cleanup job to:
            --   1. Delete old completed records in batches
            --   2. Convert invalidation_log to hypertable with retention policy
            --   3. Archive completed records to separate table for analytics
            UPDATE flexible_invalidation_log 
            SET status = 'completed',
                processed_at = NOW()
            WHERE id = log_rec.id;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Invalidation job completed. Processed % entries (job_id: %)', processed_count, job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to schedule the invalidation processing job
CREATE OR REPLACE FUNCTION schedule_invalidation_job(
    p_interval INTERVAL DEFAULT '30 seconds',
    p_batch_limit INTEGER DEFAULT 10,
    p_cagg_filter TEXT DEFAULT NULL,
    p_fixed_schedule BOOLEAN DEFAULT true
) RETURNS INTEGER AS $$
DECLARE
    job_config JSONB;
    job_id INTEGER;
BEGIN
    -- Build job configuration
    job_config := jsonb_build_object(
        'batch_limit', p_batch_limit,
        'max_age', '1 hour',
        'cagg_filter', p_cagg_filter
    );
    
    -- Schedule the job
    SELECT add_job(
        'process_flexible_invalidation_job',
        p_interval,
        config => job_config,
        fixed_schedule => p_fixed_schedule
    ) INTO job_id;
    
    RAISE NOTICE 'Scheduled invalidation processing job with ID % (interval: %, batch_limit: %)', 
                 job_id, p_interval, p_batch_limit;
    
    RETURN job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to build upsert query for continuous aggregate using catalog metadata
CREATE OR REPLACE FUNCTION upsert_query_for(
    p_cagg_name TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_partitions JSONB DEFAULT '{}'
) RETURNS TEXT AS $$
DECLARE
    v_template TEXT;
BEGIN
    -- Get the pre-generated upsert template from configuration
    SELECT upsert_query_template
    INTO v_template
    FROM cagg_configurations
    WHERE cagg_name = p_cagg_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Configuration not found for continuous aggregate %', p_cagg_name;
    END IF;
    
    -- Return the template with parameter placeholders
    -- The actual execution will bind: $1=start_time, $2=end_time, $3=partition_values
    RETURN v_template;
END;
$$ LANGUAGE plpgsql;

-- Function to execute the upsert using the stored template
CREATE OR REPLACE FUNCTION execute_upsert_for_cagg(
    p_cagg_name TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_partitions JSONB DEFAULT '{}'
) RETURNS VOID AS $$
DECLARE
    v_template TEXT;
    v_query TEXT;
BEGIN
    -- Get the stored template
    v_template := upsert_query_for(p_cagg_name, p_start_time, p_end_time, p_partitions);
    
    -- Execute with parameter binding
    EXECUTE v_template USING p_start_time, p_end_time, p_partitions;
    
    RAISE NOTICE 'Executed upsert for % from % to % with partitions %', 
                 p_cagg_name, p_start_time, p_end_time, p_partitions;
END;
$$ LANGUAGE plpgsql;


-- Status monitoring function
CREATE OR REPLACE FUNCTION get_invalidation_status()
RETURNS TABLE(
    status invalidation_status,
    count BIGINT,
    oldest_pending TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.status,
        COUNT(*) as count,
        MIN(l.created_at) as oldest_pending
    FROM flexible_invalidation_log l
    GROUP BY l.status
    ORDER BY l.status;
END;
$$ LANGUAGE plpgsql;

-- Demo: Enable comprehensive partial refresh (INSERT/UPDATE/DELETE)
SELECT enable_partial_refresh('metrics_cagg', 'tag_id', '1 hour');

-- Demo: Insert test data to trigger invalidations
INSERT INTO metrics (time, tag_id, value, device_id) VALUES 
    (NOW() - INTERVAL '30 minutes', 100, 25.0, 'device_1'),
    (NOW() - INTERVAL '30 minutes', 100, 28.0, 'device_1'),
    (NOW() - INTERVAL '25 minutes', 200, 18.5, 'device_2'),
    (NOW() - INTERVAL '20 minutes', 200, 22.0, 'device_2'),
    (NOW() - INTERVAL '15 minutes', 300, 15.5, 'device_3');

-- Demo: Update some records (should invalidate both old and new time ranges)
UPDATE metrics 
SET time = NOW() - INTERVAL '10 minutes', value = value * 1.1 
WHERE tag_id = 100 AND device_id = 'device_1';

-- Demo: Delete some records (should invalidate the deleted time ranges)
DELETE FROM metrics WHERE tag_id = 300;

-- Schedule the invalidation processing job to run every 30 seconds
SELECT schedule_invalidation_job(
    p_interval => '30 seconds',
    p_batch_limit => 10,
    p_cagg_filter => NULL,  -- Process all CAGGs
    p_fixed_schedule => true
) as scheduled_job_id;

-- Wait a moment for the job to process invalidations
SELECT pg_sleep(2);

-- Manually trigger one round of processing for immediate demo results
CALL process_flexible_invalidation_job(0, '{"batch_limit": 10, "max_age": "1 hour"}');

-- Show results
SELECT 'System Status:' as info;
SELECT * FROM get_invalidation_status();
SELECT 'Active Configurations:' as info;
SELECT cagg_name, partition_column, minimal_window FROM cagg_configurations;
SELECT 'Scheduled Jobs Status:' as info;
SELECT 'Job successfully scheduled - check TimescaleDB jobs table' as message;
SELECT 'Recent Invalidation Entries:' as info;
SELECT 
    cagg_name, 
    partition_values, 
    time_range, 
    status,
    CASE 
        WHEN batch_id IN (SELECT batch_id FROM flexible_invalidation_log GROUP BY batch_id HAVING COUNT(*) > 1) 
        THEN 'BULK (' || (SELECT COUNT(*) FROM flexible_invalidation_log fil2 WHERE fil2.batch_id = flexible_invalidation_log.batch_id)::TEXT || ' entries)'
        ELSE 'SINGLE'
    END as batch_type,
    created_at
FROM flexible_invalidation_log 
ORDER BY created_at DESC 
LIMIT 10;

-- Show results
SELECT 'System Status:' as info;
SELECT * FROM get_invalidation_status();
SELECT 'Active Configurations:' as info;
SELECT cagg_name, partition_column, minimal_window FROM cagg_configurations;

SELECT 'Complete deployment successful!' as status;

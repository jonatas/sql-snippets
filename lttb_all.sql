-- ================================================================================
-- LTTB_ALL: LTTB downsampling expression using a signal follower approach
-- ================================================================================
-- This function is a utility for users that uses the first column as LTTB column and returns all specified columns
-- Usage: SELECT * FROM lttb_all('table_name', 'main_column,other_col1,other_col2', buckets, 'WHERE condition', 'group_column')

CREATE OR REPLACE FUNCTION lttb_all(
    table_name REGCLASS,
    columns_list TEXT DEFAULT NULL,
    buckets INTEGER DEFAULT 5,
    where_clause TEXT DEFAULT '1=1',
    group_by_column TEXT DEFAULT NULL,
    timestamp_column TEXT DEFAULT 'timestamp'
) 
RETURNS SETOF RECORD AS $$
DECLARE
    query_text TEXT;
    main_column TEXT;
    all_columns TEXT[];
    columns_array TEXT[];
    select_columns TEXT;
    i INTEGER;
BEGIN
    -- Parse the columns list or auto-detect
    IF columns_list IS NOT NULL THEN
        -- Split comma-separated columns and trim whitespace
        SELECT array_agg(trim(unnest))
        INTO columns_array
        FROM unnest(string_to_array(columns_list, ','));
        
        -- First column is the LTTB column
        main_column := columns_array[1];
        
        -- Build select clause with all specified columns
        select_columns := array_to_string(columns_array, ', ');
    ELSE
        -- Auto-detect numeric columns from the table
        SELECT array_agg(c.column_name ORDER BY c.ordinal_position)
        INTO all_columns
        FROM information_schema.columns c
        WHERE c.table_name = (SELECT relname FROM pg_class WHERE oid = lttb_all.table_name)
          AND c.table_schema = 'public'
          AND c.column_name NOT IN (timestamp_column, COALESCE(group_by_column, ''))
          AND c.data_type IN ('double precision', 'numeric', 'integer', 'bigint', 'real');
        
        IF array_length(all_columns, 1) > 0 THEN
            main_column := all_columns[1];
            select_columns := array_to_string(all_columns, ', ');
        ELSE
            RAISE EXCEPTION 'No suitable numeric columns found for LTTB processing in table %', table_name;
        END IF;
    END IF;
    
    -- Build the query with flexible column selection
    query_text := format('
        WITH
        base_data AS (
            SELECT %I%s, %s FROM %s WHERE %s
        ),
        lttb_timestamps AS (
            SELECT 
                %s,
                (unnest(lttb(%I, COALESCE(%I, ''-Infinity''), %s))).time as selected_timestamp
            FROM base_data
            %s
        )
        SELECT base_data.*
        FROM lttb_timestamps
        JOIN base_data ON %s AND base_data.%I = lttb_timestamps.selected_timestamp
        ORDER BY %s, lttb_timestamps.selected_timestamp',
        
        timestamp_column,
        CASE WHEN group_by_column IS NOT NULL THEN ', ' || format('%I', group_by_column) ELSE '' END,
        select_columns,
        table_name, where_clause,
        CASE WHEN group_by_column IS NOT NULL THEN format('base_data.%I', group_by_column) ELSE '''all''' END,
        timestamp_column, main_column, buckets,
        CASE WHEN group_by_column IS NOT NULL THEN format('GROUP BY base_data.%I', group_by_column) ELSE '' END,
        CASE WHEN group_by_column IS NOT NULL THEN format('base_data.%I = lttb_timestamps.%I', group_by_column, group_by_column) ELSE '1=1' END,
        timestamp_column,
        CASE WHEN group_by_column IS NOT NULL THEN format('base_data.%I', group_by_column) ELSE 'base_data.' || timestamp_column END
    );
    
    RETURN QUERY EXECUTE query_text;
END;
$$ LANGUAGE plpgsql;

-- ================================================================================
-- MINIMAL POC DATA AND DEMONSTRATION
-- ================================================================================

-- Create minimal test table
CREATE TABLE IF NOT EXISTS sensor_data (
    timestamp TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    power DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    temperature DOUBLE PRECISION
);

-- Add humidity column if it doesn't exist
ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS humidity DOUBLE PRECISION DEFAULT 50.0;

-- Insert test data
INSERT INTO sensor_data VALUES
('2024-01-01 00:00:00+00', 'sensor_001', 100.0, 15.2, 20.1, 45.0),
('2024-01-01 00:15:00+00', 'sensor_001', 150.0, 16.1, 20.5, 47.0),
('2024-01-01 00:30:00+00', 'sensor_001', 200.0, 14.8, 19.8, 44.0),
('2024-01-01 00:45:00+00', 'sensor_001', 120.0, 15.5, 20.2, 46.0),
('2024-01-01 01:00:00+00', 'sensor_001', 110.0, 15.0, 20.0, 45.5),
('2024-01-01 00:00:00+00', 'sensor_002', 80.0, 12.1, 18.5, 52.0),
('2024-01-01 00:15:00+00', 'sensor_002', 90.0, 13.2, 19.1, 51.0),
('2024-01-01 00:30:00+00', 'sensor_002', 85.0, 12.8, 18.9, 53.0),
('2024-01-01 00:45:00+00', 'sensor_002', 95.0, 13.0, 19.0, 52.5),
('2024-01-01 01:00:00+00', 'sensor_002', 88.0, 12.5, 18.7, 51.8)
ON CONFLICT DO NOTHING;

-- ================================================================================
-- MINIMAL POC: Demonstrating comma-separated column specification
-- ================================================================================

/*
-- EXAMPLE 1: Specify columns explicitly - temperature as LTTB column, include humidity and power
SELECT * FROM lttb_all(
    'sensor_data'::regclass,
    'temperature, humidity, power',
    3,
    'sensor_id IN (''sensor_001'', ''sensor_002'')',
    'sensor_id'
) AS result(timestamp TIMESTAMPTZ, sensor_id TEXT, temperature DOUBLE PRECISION, humidity DOUBLE PRECISION, power DOUBLE PRECISION);

-- EXAMPLE 2: Simple case - just power and wind_speed
SELECT * FROM lttb_all(
    'sensor_data'::regclass,
    'power, wind_speed'
) AS result(timestamp TIMESTAMPTZ, power DOUBLE PRECISION, wind_speed DOUBLE PRECISION);

-- EXAMPLE 3: All columns with specific order
SELECT * FROM lttb_all(
    'sensor_data'::regclass,
    'wind_speed, temperature, power, humidity',
    4,
    'humidity > 48',
    'sensor_id'
) AS result(timestamp TIMESTAMPTZ, sensor_id TEXT, wind_speed DOUBLE PRECISION, temperature DOUBLE PRECISION, power DOUBLE PRECISION, humidity DOUBLE PRECISION);

-- EXAMPLE 4: Auto-detect mode (columns_list = NULL)
SELECT * FROM lttb_all(
    'sensor_data'::regclass,
    NULL,
    5,
    '1=1',
    'sensor_id'
) AS result(timestamp TIMESTAMPTZ, sensor_id TEXT, power DOUBLE PRECISION, wind_speed DOUBLE PRECISION, temperature DOUBLE PRECISION, humidity DOUBLE PRECISION);
*/
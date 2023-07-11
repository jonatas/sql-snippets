WITH caggs AS (
  SELECT
  hypertable_name, hypertable_schema,
  materialization_hypertable_schema
  || '.' || materialization_hypertable_name AS name
  FROM timescaledb_information.continuous_aggregates
  WHERE view_name = 'ny_hourly_agg'
  )
select
caggs.name,
chunk_name,
  pg_size_pretty(total_bytes) as size
  from caggs,
  public.chunks_detailed_size(caggs.name)

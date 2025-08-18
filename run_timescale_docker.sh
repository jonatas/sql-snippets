docker run -d -p 5432:5432 \
  -v data:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  timescale/timescaledb-ha:pg17.4-ts2.21.3-all

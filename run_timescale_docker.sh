docker run -d -p 5432:5432\
  -v data:/var/lib/postgresql/data\
    -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg14

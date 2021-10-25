# only if you have it installed
brew service stop postgresql

# only because you're going to do benchmarks and don't wanto to persist it
docker rm $(docker ps -aq --filter name=timescaledb)


# setup pg with password and expose the default port.
sudo docker run -d --name timescaledb -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  timescale/timescaledb:latest-pg12


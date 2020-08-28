# Local envrionment

## result:
* up:
```
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master o [0:08:07] C:130
$ docker-compose ps                 
     Name                   Command               State                               Ports                             
------------------------------------------------------------------------------------------------------------------------
cassandra-seed   docker-entrypoint.sh cassa ...   Up      7000/tcp, 7001/tcp, 7199/tcp, 0.0.0.0:9042->9042/tcp, 9160/tcp
pg-docker        docker-entrypoint.sh postg ...   Up      0.0.0.0:5430->5432/tcp
```

* down:
```
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master x [0:08:59]
$ docker-compose down
Removing fxrc_postgresql_run_9582c07bfe55 ... done
Removing pg-docker                        ... done
Removing cassandra-seed                   ... done
Removing network fxrc_default

```

## PostgreSQL from docker-compose
* ref: https://hashinteractive.com/blog/docker-compose-up-with-postgres-quick-tips/
```log
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master x [23:52:03] C:130
$ psql -h localhost -p 5430 -U docker -d blog
Password for user docker: 
psql (12.4 (Ubuntu 12.4-0ubuntu0.20.04.1), server 12.3 (Debian 12.3-1.pgdg100+1))
Type "help" for help.

blog=# \dt
         List of relations
 Schema |   Name   | Type  | Owner  
--------+----------+-------+--------
 public | app_user | table | docker
 public | post     | table | docker
(2 rows)

```

## Cassandra for docker-compose
* ref: <Microservices: Up and Running> Chapter 9. Developer Workspace
```log
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master x [23:43:20] C:130
$ docker-compose ps              
     Name                   Command               State                               Ports                             
------------------------------------------------------------------------------------------------------------------------
cassandra-seed   docker-entrypoint.sh cassa ...   Up      7000/tcp, 7001/tcp, 7199/tcp, 0.0.0.0:9042->9042/tcp, 9160/tcp
pg-docker        docker-entrypoint.sh postg ...   Up      0.0.0.0:5430->5432/tcp                                        
(base) 
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master x [23:49:42] 
$ docker exec -it cassandra-seed cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.7 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> DESCRIBE keyspaces;

system_traces  system_schema  system_auth  system  system_distributed

cqlsh> 

```
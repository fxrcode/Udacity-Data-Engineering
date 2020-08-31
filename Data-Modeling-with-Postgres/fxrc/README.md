# Local envrionment

## CLI
### Postgres
* because I installed postgres locally, so i can run `psql` directly in terminal, just need to point to correct `IP:Port`
```
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering/Data-Modeling-with-Postgres/fxrc on git:master x [23:00:58] C:130
$ psql -h localhost -p 5430 -U docker
Password for user docker: 
psql (12.4 (Ubuntu 12.4-0ubuntu0.20.04.1), server 12.3 (Debian 12.3-1.pgdg100+1))
Type "help" for help.
docker=# \l
                              List of databases
   Name    | Owner  | Encoding |  Collate   |   Ctype    | Access privileges 
-----------+--------+----------+------------+------------+-------------------
 blog      | docker | UTF8     | en_US.utf8 | en_US.utf8 | 
 docker    | docker | UTF8     | en_US.utf8 | en_US.utf8 | 
 postgres  | docker | UTF8     | en_US.utf8 | en_US.utf8 | 
 studentdb | docker | UTF8     | en_US.utf8 | en_US.utf8 | 
 template0 | docker | UTF8     | en_US.utf8 | en_US.utf8 | =c/docker        +
           |        |          |            |            | docker=CTc/docker
 template1 | docker | UTF8     | en_US.utf8 | en_US.utf8 | =c/docker        +
           |        |          |            |            | docker=CTc/docker
 udacity   | docker | UTF8     | en_US.utf8 | en_US.utf8 | 
(7 rows)

```

### Cassandra
* because I don't have Cassadra in local, so I need to use cqlsh from container
```
$ docker exec -it cassandra-seed cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.7 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh> describe keyspace udacity

CREATE KEYSPACE udacity WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

cqlsh> use udacity;

cqlsh:udacity> describe keyspace udacity

CREATE KEYSPACE udacity WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE udacity.music_library (
    year int,
    artist_name text,
    album_name text,
    PRIMARY KEY (year, artist_name)
) WITH CLUSTERING ORDER BY (artist_name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

cqlsh:udacity> select * from udacity.music_library 
           ... ;

 year | artist_name | album_name
------+-------------+-------------
 1965 | The Beatles | Rubber Soul
 1970 | The Beatles |   Let it Be

```

## Install pkg
### Postgres python wrapper
* for Psycopg, simply do this after conda activate mypy3: `pip install psycopg2-binary`
* also, in connection, remember to pass port 5430 since it's not default 5432!

### Cassandra python driver
* simply `pip install cassandra-driver`
* then you can `import cassandra`, that's it!


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
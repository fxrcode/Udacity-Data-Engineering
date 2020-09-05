# Readme

## import parch-and-posey.sql
* simple and ease.
```sql
# fxrc @ pop in ~/Learn/UdacityNanodegree/Udacity-Data-Engineering on git:master x [17:38:55]
$ psql -h localhost -p 5432 -U student studentdb
Password for user student: 
psql (12.4 (Ubuntu 12.4-0ubuntu0.20.04.1), server 12.3 (Debian 12.3-1.pgdg100+1))
Type "help" for help.
                                 
studentdb=# \l
                               List of databases
   Name    |  Owner  | Encoding |  Collate   |   Ctype    |  Access privileges  
-----------+---------+----------+------------+------------+---------------------
 postgres  | student | UTF8     | en_US.utf8 | en_US.utf8 | 
 studentdb | student | UTF8     | en_US.utf8 | en_US.utf8 | 
 template0 | student | UTF8     | en_US.utf8 | en_US.utf8 | =c/student         +
           |         |          |            |            | student=CTc/student
 template1 | student | UTF8     | en_US.utf8 | en_US.utf8 | =c/student         +
           |         |          |            |            | student=CTc/student
(4 rows)  
                                 
studentdb=# ls
studentdb-# \dt
Did not find any relations.
studentdb-# \i Extra-Curricular/SQL/parch-and-posey.sql 
BEGIN     
CREATE TABLE
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1     
INSERT 0 1                  
INSERT 0 1

....
....  ## 16000 line
....
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
COMMIT
studentdb-# \dt
           List of relations
 Schema |    Name    | Type  |  Owner  
--------+------------+-------+---------
 public | accounts   | table | student
 public | orders     | table | student
 public | region     | table | student
 public | sales_reps | table | student
 public | web_events | table | student
(5 rows)

studentdb-#
studentdb=# select count(*) from accounts;
 count
-------
   351
(1 row)

studentdb=# select count(*) from orders;
 count
-------
  6912
(1 row)

studentdb=# select count(*) from web_events;
 count
-------
  9073
(1 row)

studentdb=# select count(*) from region;
 count
-------
     4
(1 row)

studentdb=# select count(*) from sales_reps;
 count
-------
    50
(1 row)


```
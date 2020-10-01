# Capstone Udacity Project

## General Flow
* go for Data Lake Spark track.
* Create interative local Spark jupyter notebook for development with partial dataset. Run `start_local_spark.sh` in local.
* Run python notebook in EMR notebook, then check parquet result in HDFS folder. 

## Detail Flow
### 1. Extract data
* airport: small csv
```
root
 |-- ident: string (nullable = true)
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- elevation_ft: integer (nullable = true)
 |-- continent: string (nullable = true)
 |-- iso_country: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- municipality: string (nullable = true)
 |-- gps_code: string (nullable = true)
 |-- iata_code: string (nullable = true)
 |-- local_code: string (nullable = true)
 |-- coordinates: string (nullable = true)

+-----+-------------+--------------------+------------+---------+-----------+----------+------------+--------+---------+----------+--------------------+
|ident|         type|                name|elevation_ft|continent|iso_country|iso_region|municipality|gps_code|iata_code|local_code|         coordinates|
+-----+-------------+--------------------+------------+---------+-----------+----------+------------+--------+---------+----------+--------------------+
|  00A|     heliport|   Total Rf Heliport|          11|       NA|         US|     US-PA|    Bensalem|     00A|     null|       00A|-74.9336013793945...|
| 00AA|small_airport|Aero B Ranch Airport|        3435|       NA|         US|     US-KS|       Leoti|    00AA|     null|      00AA|-101.473911, 38.7...|
```

* gloabal temperatures by city: 500MB csv
```
root
 |-- dt: timestamp (nullable = true)
 |-- AverageTemperature: double (nullable = true)
 |-- AverageTemperatureUncertainty: double (nullable = true)
 |-- City: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)

+-------------------+------------------+-----------------------------+-----+-------+--------+---------+
|                 dt|AverageTemperature|AverageTemperatureUncertainty| City|Country|Latitude|Longitude|
+-------------------+------------------+-----------------------------+-----+-------+--------+---------+
|1743-11-01 00:00:00|             6.068|           1.7369999999999999|Århus|Denmark|  57.05N|   10.33E|
|1743-12-01 00:00:00|              null|                         null|Århus|Denmark|  57.05N|   10.33E|
```

* demographs: small csv
```
root
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- Median Age: double (nullable = true)
 |-- Male Population: integer (nullable = true)
 |-- Female Population: integer (nullable = true)
 |-- Total Population: integer (nullable = true)
 |-- Number of Veterans: integer (nullable = true)
 |-- Foreign-born: integer (nullable = true)
 |-- Average Household Size: double (nullable = true)
 |-- State Code: string (nullable = true)
 |-- Race: string (nullable = true)
 |-- Count: integer (nullable = true)

+----------------+-------------+----------+---------------+-----------------+----------------+------------------+------------+----------------------+----------+--------------------+-----+
|            City|        State|Median Age|Male Population|Female Population|Total Population|Number of Veterans|Foreign-born|Average Household Size|State Code|                Race|Count|
+----------------+-------------+----------+---------------+-----------------+----------------+------------------+------------+----------------------+----------+--------------------+-----+
|   Silver Spring|     Maryland|      33.8|          40601|            41862|           82463|              1562|       30908|                   2.6|        MD|  Hispanic or Latino|25924|
|          Quincy|Massachusetts|      41.0|          44129|            49500|           93629|              4147|       32935|                  2.39|        MA|               White|58723|
```
* i94 data: 6GB sas data -> need to convert (sampled) and saved into parquet
```
root
 |-- cicid: double (nullable = true)
 |-- i94yr: double (nullable = true)
 |-- i94mon: double (nullable = true)
 |-- i94cit: double (nullable = true)
 |-- i94res: double (nullable = true)
 |-- i94port: string (nullable = true)
 |-- arrdate: double (nullable = true)
 |-- i94mode: double (nullable = true)
 |-- i94addr: string (nullable = true)
 |-- depdate: double (nullable = true)
 |-- i94bir: double (nullable = true)
 |-- i94visa: double (nullable = true)
 |-- count: double (nullable = true)
 |-- dtadfile: string (nullable = true)
 |-- visapost: string (nullable = true)
 |-- occup: string (nullable = true)
 |-- entdepa: string (nullable = true)
 |-- entdepd: string (nullable = true)
 |-- entdepu: string (nullable = true)
 |-- matflag: string (nullable = true)
 |-- biryear: double (nullable = true)
 |-- dtaddto: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- insnum: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- admnum: double (nullable = true)
 |-- fltno: string (nullable = true)
 |-- visatype: string (nullable = true)
 
 +---------+------+------+------+------+-------+-------+-------+-------+-------+------+-------+-----+--------+--------+-----+-------+-------+-------+-------+-------+--------+------+------+-------+-------------+-----+--------+
|    cicid| i94yr|i94mon|i94cit|i94res|i94port|arrdate|i94mode|i94addr|depdate|i94bir|i94visa|count|dtadfile|visapost|occup|entdepa|entdepd|entdepu|matflag|biryear| dtaddto|gender|insnum|airline|       admnum|fltno|visatype|
+---------+------+------+------+------+-------+-------+-------+-------+-------+------+-------+-----+--------+--------+-----+-------+-------+-------+-------+-------+--------+------+------+-------+-------------+-----+--------+
|6934633.0|2016.0|   7.0| 582.0| 582.0|    ATL|20664.0|    1.0|     PA|20670.0|  41.0|    2.0|  1.0|20160804|    null| null|      G|      O|   null|      M| 1975.0|01282017|     M|179624|     DL|3.858921085E9|  130|      B2|
|6934636.0|2016.0|   7.0| 116.0| 116.0|    NEW|20664.0|    1.0|     FL|20678.0|  46.0|    2.0|  1.0|20160813|    null| null|      G|      O|   null|      M| 1970.0|10262016|     M|669388|     UA|3.858924885E9|   76|      WT|
|6934639.0|2016.0|   7.0| 148.0| 112.0|    NEW|20664.0|    1.0|     NY|20671.0|  32.0|    1.0|  1.0|20160805|    null| null|      G|      O|   null|      M| 1984.0|10262016|     F|335420|     UA|3.845095085E9|  865|      WB|
 ```

### 2. Transform data
* i94visa: `df_i94visa.parquet`
```
root
 |-- id: long (nullable = true)
 |-- visa_type: string (nullable = true)

+---+---------+
| id|visa_type|
+---+---------+
|  1| BUSINESS|
|  2| PLEASURE|
|  3|  STUDENT|
```

* i94port: `df_i94port.parquet`
```
root
 |-- id: string (nullable = true)
 |-- airport: string (nullable = true)

+---+--------------------+
| id|             airport|
+---+--------------------+
|ALC|           ALCAN, AK|
|ANC|       ANCHORAGE, AK|
|BAR|BAKER AAF - BAKER...|
|DAC|   DALTONS CACHE, AK|
```

* airport: `df_airport.parquet`
```
root
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- iata_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)

+-------------+--------------------+----------+---------+------------------+-------------------+
|         type|                name|iso_region|iata_code|          latitude|          longitude|
+-------------+--------------------+----------+---------+------------------+-------------------+
|small_airport|Ocean Reef Club A...|     US-FL|      OCA|  -80.274803161621|    25.325399398804|
|small_airport|Pilot Station Air...|     US-AK|      PQS|       -162.899994|          61.934601|
|small_airport|Crested Butte Air...|     US-CO|      CSE|       -106.928341|          38.851918|
```

* i94cit: `df_i94cit.parquet`
```
root
 |-- id: long (nullable = true)
 |-- citres: string (nullable = true)

+---+--------------------+
| id|              citres|
+---+--------------------+
|582|MEXICO AIR SEA, A...|
|236|         AFGHANISTAN|
|101|             ALBANIA|
|316|             ALGERIA|
```


* i94addr: `df_i94addr.parquet`
```
root
 |-- st: string (nullable = true)
 |-- state: string (nullable = true)

+---+-----------------+
| st|            state|
+---+-----------------+
| AL|          ALABAMA|
| AK|           ALASKA|
| AZ|          ARIZONA|
```

* demograph: `df_demo.parquet`
```
root
 |-- State_Code: string (nullable = true)
 |-- Median_Age: double (nullable = true)
 |-- Male_Population: long (nullable = true)
 |-- Female_Population: long (nullable = true)
 |-- Total_Population: long (nullable = true)
 |-- Number_Veterans: long (nullable = true)
 |-- Foregin_born: long (nullable = true)
 |-- Average_Household: double (nullable = true)
 |-- American_Indian_Alaska_Native: long (nullable = true)
 |-- Asian: long (nullable = true)
 |-- Black_African-American: long (nullable = true)
 |-- Hispanic_Latino: long (nullable = true)
 |-- White: long (nullable = true)

+----------+------------------+---------------+-----------------+----------------+---------------+------------+------------------+-----------------------------+-------+----------------------+---------------+--------+
|State_Code|        Median_Age|Male_Population|Female_Population|Total_Population|Number_Veterans|Foregin_born| Average_Household|American_Indian_Alaska_Native|  Asian|Black_African-American|Hispanic_Latino|   White|
+----------+------------------+---------------+-----------------+----------------+---------------+------------+------------------+-----------------------------+-------+----------------------+---------------+--------+
|        SC| 34.17999999999999|         260944|           272713|          533657|          33463|       27744|             2.472|                         3705|  13355|                175064|          29863|  343764|
|        AZ|           35.0375|        2227455|          2272087|         4499542|         264505|      682313|          2.774375|                       129708| 229183|                296222|        1508157| 3591611|
|        LA|            34.625|         626998|           673597|         1300595|          69771|       83419|             2.465|                         8263|  38739|                602377|          87133|  654578|
|        MN| 35.61818181818182|         702157|           720246|         1422403|          64894|      215873|2.5009090909090914|                        25242| 151544|                216731|         103229| 1050239|
```

* i94: `i94_valid.parquet`

```
root
 |-- cicid: integer (nullable = true)
 |-- i94yr: integer (nullable = true)
 |-- i94mon: integer (nullable = true)
 |-- i94cit: integer (nullable = true)
 |-- i94res: integer (nullable = true)
 |-- i94port: string (nullable = true)
 |-- arrdate: integer (nullable = true)
 |-- i94addr: string (nullable = true)
 |-- depdate: integer (nullable = true)
 |-- i94visa: integer (nullable = true)
 |-- biryear: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- visatype: string (nullable = true)

+-----+-----+------+------+------+-------+-------+-------+-------+-------+-------+------+-------+--------+
|cicid|i94yr|i94mon|i94cit|i94res|i94port|arrdate|i94addr|depdate|i94visa|biryear|gender|airline|visatype|
+-----+-----+------+------+------+-------+-------+-------+-------+-------+-------+------+-------+--------+
| 2187| 2016|    12|   209|   209|    AGA|  20789|     FL|   null|      2|   1957|     M|     UA|     GMT|
| 5708| 2016|    12|   209|   209|    AGA|  20789|     NY|  20793|      2|   1987|     F|     DL|     GMT|
| 9882| 2016|     8|   582|   582|    LVG|  20667|     NV|  20670|      2|   1952|     M|     4O|      B2|
|19222| 2016|    12|   245|   245|    LOS|  20789|     CA|  20796|      2|   1980|     F|     DL|      B2|
|24161| 2016|     8|   135|   135|    SPM|  20667|     TX|  20676|      2|   1994|     M|     EV|      WT|
```

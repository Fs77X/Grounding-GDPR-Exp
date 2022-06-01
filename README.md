# Grounding-GDPR-Exp

## Delete Experiments
This branch contains the code to run the experiments from figure 1. 

## Prerequisites
1. openjdk 11.0.15 2022-04-19 (sudo apt install openjdk-11-jre-headless on debian oses)
2. go version go1.17.6 linux/amd64 or higher (brew install golang)
3. PostgreSQL v14.3 (https://www.postgresql.org/download/)
4. Apache Maven 3.6.3 (sudo apt install maven on debian oses)

## How to run
NOTE: Experiments were ran with CSVLog enabled in PostgreSQL

Resource for instructions on enabling CSVLog:
https://www.loggly.com/use-cases/postgresql-logs-logging-setup-and-troubleshooting/

Edit the file /etc/postgresql/14/main/pg_hba.conf and replace the three type values below the comment:
```bash 
# "local" is for Unix domain socket connections only
``` 
 with METHOD value of trust.

Edit the file /etc/postgresql/14/main/postgresql.conf with the following max_connection:
```bash
max_connections = 100000
```
This allows postgresql to accpet many connections very fast which is needed for the benchmark. Also, make sure to restart postgresql once you've made all of these changes!

In a terminal run the following as the postgres user:
```bash
sudo su postgres # to switch to postgres
psql
CREATE USER admin WITH LOGIN;
CREATE DATABASE the_db;
GRANT ALL PRIVILEGES ON DATABASE the_db TO admin;
exit #CTRL+D
exit #exit postgres user
```

To load the table:
```bash
psql -U admin the_db
# If running the tombstone experiments, run the create table command below.
CREATE TABLE usertable(id character varying(50) NOT NULL,
  shop_name character varying(20) NOT NULL,
  obs_date character varying(255) NOT NULL,
  obs_time character varying(255) NOT NULL,
  user_interest character varying(20),
  device_id integer NOT NULL,
  querier character varying(255) NOT NULL,
  purpose character varying(255) NOT NULL,
  ttl integer NOT NULL,
  origin character varying(255) NOT NULL,
  objection character varying(255) NOT NULL,
  sharing character varying(255) NOT NULL,
  enforcement_action character varying(255),
  inserted_at character varying(255) NOT NULL
  );

# FOR TOMBSTONE EXPERIMENT
CREATE TABLE usertable(id character varying(50) NOT NULL,
  shop_name character varying(20) NOT NULL,
  obs_date character varying(255) NOT NULL,
  obs_time character varying(255) NOT NULL,
  user_interest character varying(20),
  device_id integer NOT NULL,
  querier character varying(255) NOT NULL,
  purpose character varying(255) NOT NULL,
  ttl integer NOT NULL,
  origin character varying(255) NOT NULL,
  objection character varying(255) NOT NULL,
  sharing character varying(255) NOT NULL,
  enforcement_action character varying(255),
  inserted_at character varying(255) NOT NULL,
  tomb integer NOT NULL
  );
```

Download the following jar files:
- https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar
- https://repo1.maven.org/maven2/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar 

Open a terminal:
```bash
cd GDPRbench/src/
mvn clean package
```
Place the two jar files in GDPRbench/src/core/target/

In the same terminal window run the following command to load the generated dataset
```bash
<start postgres if you havent>
./bin/ycsb.sh load jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{controller workload from workload directory} -s
```

Open another terminal when the loading is done:
```bash
cd Grounding-GDPR-Exp/ttlscript
go run ttldaemon.go {delete, vac, vacfull, tomb} # depending on the experiment, if you want to run normal deletes then give the delete argument for example
```

In the terminal where you loaded the dataset, run the experiment with the following command

```bash
./bin/ycsb.sh run jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{controller workload from workload directory} -s
```

Output from the benchmark should give eta's as the benchmark runs and overall runtime in seconds and ms, throughtput and latency measurements for each type of operation in the workload.

## Cleanup
After the benchmark is done running, delete the data from usertable and do a full vacuum. To switch from the base delete, vacuum and full vacuum experiments to the tomb experiments run the following SQL command:

```bash
ALTER TABLE usertable ADD COLUMN tomb integer;
```
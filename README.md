# Grounding-GDPR-Exp

## P1YCSBBenchmark Experiments
This branch contains the code to run the experiments from figure 5. 

## Prerequisites
1. openjdk 11.0.15 2022-04-19 (sudo apt install openjdk-11-jre-headless on debian oses)
2. go version go1.17.6 linux/amd64 or higher (brew install golang)
3. PostgreSQL v14.3 (https://www.postgresql.org/download/)
4. Apache Maven 3.6.3 (sudo apt install maven on debian oses)
5. Encrypted disk using OpenZFS AES-256-GCM (https://openzfs.github.io/openzfs-docs/Getting%20Started/Ubuntu/Ubuntu%2020.04%20Root%20on%20ZFS.html)

NOTE: For Prereq 5, it is recommended to use the ubuntu 20.04 installer and follow the methods as shown in the section for the ubuntu installer. Not only is it much easier but much faster! :) 

## How to run
NOTE: CSVLog is turned off for this system. You can simply comment the lines that enabled logging and restart psql if you enabled it prior.

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
CREATE ROLE admin WITH LOGIN;
CREATE ROLE controller WITH LOGIN;
CREATE ROLE customer WITH LOGIN;
CREATE ROLE processor WITH LOGIN;
CREATE DATABASE the_db;
GRANT ALL PRIVILEGES ON DATABASE the_db TO admin;
exit #CTRL+D
exit #exit postgres user
```

To load the table:
```bash
psql -U admin the_db
CREATE TABLE usertable(id character varying(50) NOT NULL,
  shop_name character varying(20) NOT NULL,
  obs_date character varying(255) NOT NULL,
  obs_time character varying(255) NOT NULL,
  user_interest character varying(20),
  device_id integer NOT NULL
  );
CREATE POLICY admin_controller ON usertable TO controller USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO controller;
CREATE POLICY admin_processor ON usertable TO processor USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO processor;
CREATE POLICY admin_customer ON usertable TO customer USING (true) WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON usertable TO customer;
ALTER table usertable enable row level security;
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

For the p1 system, we have additional logging which can be ran in a seperate terminal window:
```bash
cd Grounding-GDPR-Exp/logscript
go run logRes.go
```

In the same terminal window when compiling the project with maven run the following command to load the generated dataset
```bash
<start postgres if you havent>
./bin/ycsb.sh load jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{controller workload from workload directory} -s
```

NOTE: go may tell you to run a command to fetch the library in order to be able to run the script. Make sure to do that so you're able to run it!

In the terminal where you loaded the dataset, run the experiment with the following command

```bash
./bin/ycsb.sh run jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{workload from workload directory} -s
```

Output from the benchmark should give eta's as the benchmark runs and overall runtime in seconds and ms, throughtput and latency measurements for each type of operation in the workload.

## Cleanup
After the benchmark is done running, delete the data from usertable and userpolicy and also do a full vacuum. Also, close the ttl script and log script and rerun them as this was the routine used while running the experiments.
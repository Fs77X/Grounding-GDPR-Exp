# Grounding-GDPR-Exp

## P2YCSB Benchmark Experiments
This branch contains the code to run the experiments from figure 5c. 

## Prerequisites
1. openjdk 11.0.15 2022-04-19 (sudo apt install openjdk-11-jre-headless on debian oses)
2. go version go1.17.6 linux/amd64 or higher (brew install golang)
3. PostgreSQL v14.3 (https://www.postgresql.org/download/)
4. Apache Maven 3.6.3 (sudo apt install maven on debian oses)
5. Encrypted disk using OpenZFS AES-128-CCM (https://openzfs.github.io/openzfs-docs/Getting%20Started/Ubuntu/Ubuntu%2020.04%20Root%20on%20ZFS.html)
6. Python 3 (v3.9)
7. nodejs (v10.19.0) (through sudo apt install nodejs npm)
8. Apache Kafka (https://kafka.apache.org/quickstart)

NOTE: For Prereq 5, it is recommended to use the ubuntu 20.04 installer and follow the methods as shown in the section for the ubuntu installer. Not only is it much easier but much faster! :) Also, make sure to replace the encryption algorithm with AES-128-CCM over AES-256-GCM!

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
CREATE ROLE sieve WITH LOGIN;
CREATE DATABASE sieve;
GRANT ALL PRIVILEGES ON DATABASE sieve TO sieve;
exit #CTRL+D
exit #exit postgres user
```

To load the tables:
```bash
psql -U sieve sieve
CREATE TABLE public.mall_observation (
    id character varying(50) NOT NULL,
    shop_name character varying(20) NOT NULL,
    obs_date date NOT NULL,
    obs_time time without time zone NOT NULL,
    user_interest character varying(20),
    device_id integer NOT NULL
);


ALTER TABLE public.mall_observation OWNER TO sieve;
```


Open two terminals to start Kafka:
```bash
cd kafka_2.13-3.2.0
# in the first terminal
bin/zookeeper-server-start.sh config/zookeeper.properties
# in the second terminal
bin/kafka-server-start.sh config/server.properties
```

In the same kafka directory run these commands:
```bash
bin/kafka-topics.sh --create --topic query --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic logResults --bootstrap-server localhost:9092
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

For the p2 system, we have additional logging which can be ran in a seperate terminal window:
```bash
cd Grounding-GDPR-Exp/logscript
# for first time run the command below
pip3 install kafka-python
python3 kafkalog.py
```

To get sieve started run the following assuming you're in the root directory of the repository:
```bash
mvn clean install
mvn exec:java
```

To get the middleware between sieve and the client, run the following:
```bash
cd endpoint
npm install
npm run dev
```

In the same terminal window when compiling the project with maven run the following command to load the generated dataset
```bash
<start postgres if you havent>
./bin/ycsb.sh load jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{workload from workload directory} -s
```

In the terminal where you loaded the dataset, run the experiment with the following command

```bash
./bin/ycsb.sh run jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/{workload from workload directory} -s
```

Output from the benchmark should give eta's as the benchmark runs and overall runtime in seconds and ms, throughtput and latency measurements for each type of operation in the workload.

## Cleanup
Because sieve has multiple tables it is easier to drop the entire database and recreate it from the steps in the readme. Also, close the ttl script and log script and rerun them as this was the routine used while running the experiments. To reset kafka run the following command after shutting down kafka and zookeeper:

```bash
rm -rf /tmp/kafka-logs /tmp/zookeeper
```
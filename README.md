# Grounding-GDPR-Exp

## Delete Experiments
This branch contains the code to run the experiments from figure 1. 

## Prerequisites
1. openjdk 11.0.15 2022-04-19
2. go version go1.17.6 linux/amd64 or higher
3. PostgreSQL v14.3

## How to run
Download the following jar files:
- https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar
- https://repo1.maven.org/maven2/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar 
Open a terminal:

Open one terminal:
```bash
cd GDPRbench/src/
mvn clean package
<start redis or postgres>
configure workloads/gdpr_{controller|customer|processor|regulator}
./bin/ycsb load redis -s -P workloads/gdpr_controller (for redis)
./bin/ycsb run redis -s -P workloads/gdpr_controller
```
# GDPRbench

The General Data Protection Regulation (GDPR) was introduced in Europe to offer new rights and protections to people concerning their personal data. In this project, we aim to benchmark how well a given storage system responds to the common queries of GDPR. In order to do this, we [identify](images/gdpr-workloads.png) four key roles in GDPR--customer, controller, processor, and regulator--and compose workloads corresponding to their functionalities. The design of this benchmark is guided by our analysis of GDPR as well as the usage patterns from the real-world.

## Design and Implementation

We implement GDPRbench by adapting and extending YCSB. This [figure](images/gdprbench.png) shows the core infrastructure components of YCSB (in gray), and our modifications and extensions (in blue). We create four new workloads, a GDPR-specific workload executor, and implement DB clients (one per storage system). So far, we have added ~1300 LoC to the workload engine, and ∼400 LoC for Redis and PostgreSQL clients.

## Benchmarking

To get started with GDPRbench, download or clone this repository. It consists of a fully functional version of YCSB together with all the functionalities of GDPRbench. Please note that you will need [Maven 3](https://maven.apache.org/) to build and use the benchmark.

```bash
git clone https://github.com/GDPRbench/GDPRbench.git
cd GDPRbench/src/
mvn clean package
<start redis or postgres>
configure workloads/gdpr_{controller|customer|processor|regulator}
./bin/ycsb load redis -s -P workloads/gdpr_controller
./bin/ycsb run redis -s -P workloads/gdpr_controller
./bin/ycsb.sh load jdbc -P ./jdbc/src/main/conf/db.properties -P ./workloads/gdpr_controller -s > load100k10kCont 
```

Interested in exploring the research behind this project? Check out our [website](https://gdprbench.org/).
To do before running commands:
1. Download the following jars:
 - https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar 
 - https://repo1.maven.org/maven2/org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar 
2. Create directory in /GDPRbench/src/core/target called "dependency" (without quotes)
3. place into new folder
4. run program

If running into filepointer limit issues do this:
Run these commands in all terminals running benchmark, middleware and logservice in order for the benchmark to run.
1. sudo sysctl fs.nr_open=10000000
2. sudo sysctl fs.file-max=10000000
3. mylimit=10000000
4. sudo prlimit --nofile=$mylimit --pid $$; ulimit -n $mylimit

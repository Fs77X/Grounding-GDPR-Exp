# Grounding-GDPR-Exp
Welcome to the repo!

We have split the repo into different branches for each of the experiments/systems. 

List of branches:
- DeleteExp
- p_base
- p_base_ycsb
- P_UCI
- P_UCI_ycsb
- P_gbench
- P_gbench_ycsb

## Example Output from GDPRBench
```bash
2022-05-28 05:47:16:091 580 sec: 10000 operations; 23.29 current ops/sec; [DELETE: Count=6, Max=61375, Min=53376, Avg=56762.67, 90=59263, 99=61375, 99.9=61375, 99.99=61375] [CLEANUP: Count=1, Max=74, Min=74, Avg=74, 90=74, 99=74, 99.9=74, 99.99=74] [INSERT: Count=5, Max=19247, Min=9640, Avg=12574.4, 90=19247, 99=19247, 99.9=19247, 99.99=19247] [UPDATEMETA: Count=5, Max=69631, Min=61184, Avg=64921.6, 90=69631, 99=69631, 99.9=69631, 99.99=69631] 
[OVERALL], RunTime(ms), 580688
[OVERALL], Throughput(ops/sec), 17.220951698674675
[TOTAL_GCS_G1_Young_Generation], Count, 985
[TOTAL_GC_TIME_G1_Young_Generation], Time(ms), 2264
[TOTAL_GC_TIME_%_G1_Young_Generation], Time(%), 0.3898823464579947
[TOTAL_GCS_G1_Old_Generation], Count, 0
[TOTAL_GC_TIME_G1_Old_Generation], Time(ms), 0
[TOTAL_GC_TIME_%_G1_Old_Generation], Time(%), 0.0
[TOTAL_GCs], Count, 985
[TOTAL_GC_TIME], Time(ms), 2264
[TOTAL_GC_TIME_%], Time(%), 0.3898823464579947
[DELETE], Operations, 2463
[DELETE], AverageLatency(us), 67772.34916768169
[DELETE], MinLatency(us), 53312
[DELETE], MaxLatency(us), 199807
[DELETE], 95thPercentileLatency(us), 85759
[DELETE], 99thPercentileLatency(us), 97151
[DELETE], Return=OK, 2463
[CLEANUP], Operations, 1
[CLEANUP], AverageLatency(us), 74.0
[CLEANUP], MinLatency(us), 74
[CLEANUP], MaxLatency(us), 74
[CLEANUP], 95thPercentileLatency(us), 74
[CLEANUP], 99thPercentileLatency(us), 74
[INSERT], Operations, 2504
[INSERT], AverageLatency(us), 12750.261980830672
[INSERT], MinLatency(us), 8472
[INSERT], MaxLatency(us), 64031
[INSERT], 95thPercentileLatency(us), 17215
[INSERT], 99thPercentileLatency(us), 24511
[INSERT], Return=OK, 2504
[UPDATEMETA], Operations, 5033
[UPDATEMETA], AverageLatency(us), 75791.42459765547
[UPDATEMETA], MinLatency(us), 58496
[UPDATEMETA], MaxLatency(us), 158847
[UPDATEMETA], 95thPercentileLatency(us), 95359
[UPDATEMETA], 99thPercentileLatency(us), 106943
[UPDATEMETA], Return=OK, 5033
```
The seconds mentioned after the time is what is recorded for the graphs.

NOTE: Sometimes one or two operations may fail as it will report it after the benchmark is finished running a workload. This is normal as sometimes the data may have expired by the time the benchmark requests it. 

## Experiment Graph Recreation
In the main branch includes the notebook to recreate the graphs which are also included in the repo 

To run the notebook you must have the following python packages:
- numpy
- pandas
- seaborn
- matplotlib

## Graphs from script
![Figure1](/Figure1.png "Figure 1 Graph")

![Figure5a](/Figure5a.png "Figure 5a Graph")

![Figure5b](/Figure5b.png "Figure 5b Graph")

![Figure5c](/Figure5c.png "Figure 5c Graph")

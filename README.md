# Local-MapReduce  
## About
Local-MapReduce is inspired by the map reduce paradigm for computation of large data. Basic idea behind this implementation is to fork multiple child processes to perform map and reduce tasks which are monitored by a master process and all the communication between the processes (parent-child or siblings) are done using pipes.  
## Design

![Design](local_mapreduce.png)
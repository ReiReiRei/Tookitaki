# Tookitaki
## Requirements
* Sbt build tool 0.13.16
* JDK8+
* Apcahe Spark 2.2.0 distribution
## How to build
```sbt assembly```
Output jar is tookitaki-assembly-0.1.jar
## How to run
You should download raw data and place csv files in separate directory (input directory).
  NOTE RAW DATA IS NOT INCLUDED IN THIS REPO)
  You should provide batch of csv files in separate dir(non recursive).

If you use hdfs as file system each spark executor should be configrated as a machine of hadoop cluster
```
spark-submit --master local[*] --class reireiei.tookitaki.Main target/scala-2.11/tookitaki-assembly-0.1.jar --input ./input  --output ./result
```
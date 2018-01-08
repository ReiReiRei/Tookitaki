# Tookitaki
## Requirements
* Sbt build tool 0.13.16
* JDK8+
* Apcahe Spark 2.2.1 distribution
## How to build
```sbt assembly```
Output jar is tookitaki-assembly-0.1.jar
## How to run
You should download raw data and place csv files in separate directory (input directory).
If you use hdfs as file system each spark executor should be configrated as a machine of hadoop cluster
```
spark-submit --master local[*] --class reireiei.tookitaki.Main --verbose target/scala-2.11/tookitaki-assembly-0.1.jar --input ./input  --output ./result
```
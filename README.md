# Distributed Weather Anomaly Detection using Hadoop MapReduce
### BS Class: Parallel & Distributed Computing Mini-Project (Group 06)

This repository contains the complete implementation and documentation for Group 06's PDC mini-project: **Global Weather Anomaly Detection**. The project calculates a 10-year baseline mean and standard deviation ($\sigma$) per weather station using the NOAA Global Surface Summary of the Day (GSOD) dataset, and detects extreme weather events (anomaly days) where temperatures deviate by more than $3\sigma$.

---

## 1. System Architecture

Below is the conceptual architecture of the physical multi-node master/worker cluster and the two-stage MapReduce pipeline:

```mermaid
graph TD
    subgraph Cluster Infrastructure
        MasterNode[Master Node<br>NameNode / ResourceManager] <-->|LAN / Wi-Fi| Worker1[Worker Node 1<br>DataNode / NodeManager]
        MasterNode <-->|LAN / Wi-Fi| Worker2[Worker Node 2<br>DataNode / NodeManager]
    end

    subgraph MapReduce Pipeline
        RawData[(NOAA GSOD Dataset)] --> Stage1[Stage 1: StatsCalculationJob]
        Stage1 -->|Map: Parse Temp| Comb1[Combiner: Local Sum & Count]
        Comb1 -->|Reduce: Aggregation| StatsFile[Baseline Stats<br>Station ID -> Mean, Sigma]
        
        StatsFile -->|Distributed Cache| Stage2[Stage 2: AnomalyDetectionJob]
        RawData --> Stage2
        Stage2 -->|Map-Only: Filter temp > Mean + 3*Sigma| AnomalyFile[Anomaly Output<br>STATION, DATE, TEMP, MEAN, SIGMA, DEVIATION]
    end
```

---

## 2. Infrastructure Setup: Physical Multi-Node Cluster

Follow these steps to establish a physical distributed Hadoop and Spark cluster using multiple laptops.

### 2.1 Network & Hosts Configuration
All laptops must be connected to the same LAN or Wi-Fi network.

1. **Assign Static IPs** to each laptop (e.g., in your router settings or OS network settings).
2. **Edit Host Files** on every machine. Add mappings for the master and worker nodes:
   - On **Linux/macOS**: Edit `/etc/hosts`
   - On **Windows**: Edit `C:\Windows\System32\drivers\etc\hosts`
   
   Add the following lines (adjust the IP addresses to match your network):
   ```text
   192.168.1.100  master-node
   192.168.1.101  worker-node1
   192.168.1.102  worker-node2
   ```

### 2.2 SSH Keyless Login Configuration
The master node must be able to log in to all worker nodes (and itself) without password prompts.

1. On the **Master Node**, generate an SSH key pair:
   ```bash
   ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
   ```
2. Copy the public key to the master's authorized keys list:
   ```bash
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 0600 ~/.ssh/authorized_keys
   ```
3. Copy the public key to all worker nodes:
   ```bash
   ssh-copy-id -i ~/.ssh/id_rsa.pub user@worker-node1
   ssh-copy-id -i ~/.ssh/id_rsa.pub user@worker-node2
   ```
4. Verify by running `ssh worker-node1` from the master. It should connect instantly without asking for a password.

---

## 3. Software Installation & Configuration

### 3.1 Java JDK Installation
Hadoop and Spark require Java. Install **Java JDK 8 or 11** on all nodes.
- Set the `JAVA_HOME` environment variable on all machines:
  - Linux example in `~/.bashrc`:
    ```bash
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    export PATH=$PATH:$JAVA_HOME/bin
    ```

### 3.2 Hadoop Installation
1. Download Hadoop 3.3.6 on all nodes and extract it to a directory, e.g., `/usr/local/hadoop`.
2. Define environment variables in `~/.bashrc` on all nodes:
   ```bash
   export HADOOP_HOME=/usr/local/hadoop
   export HADOOP_INSTALL=$HADOOP_HOME
   export HADOOP_MAPRED_HOME=$HADOOP_HOME
   export HADOOP_COMMON_HOME=$HADOOP_HOME
   export HADOOP_HDFS_HOME=$HADOOP_HOME
   export YARN_HOME=$HADOOP_HOME
   export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
   export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
   export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
   ```
   Apply the changes: `source ~/.bashrc`.

### 3.3 Hadoop Configuration Files
On the **Master Node**, edit the configuration files inside `$HADOOP_HOME/etc/hadoop/`:

#### A. `hadoop-env.sh`
Uncomment and set `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64` (or your actual JDK path).

#### B. `core-site.xml`
Defines the default HDFS URI and temporary file path.
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master-node:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/hadoop/tmp</value>
    </property>
</configuration>
```

#### C. `hdfs-site.xml`
Defines the replication factor (typically 2 or 3 depending on node count) and locations for name and data nodes.
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/usr/local/hadoop/data/dfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/usr/local/hadoop/data/dfs/datanode</value>
    </property>
</configuration>
```

#### D. `mapred-site.xml`
Sets the MapReduce framework to YARN.
```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
```

#### E. `yarn-site.xml`
Configures resource management parameters.
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master-node</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

#### F. `workers`
Add the hostnames of the worker nodes (replace `localhost`):
```text
worker-node1
worker-node2
```

> [!TIP]
> Sync these configuration files from the Master to all Worker nodes:
> ```bash
> scp -r $HADOOP_HOME/etc/hadoop/* user@worker-node1:$HADOOP_HOME/etc/hadoop/
> scp -r $HADOOP_HOME/etc/hadoop/* user@worker-node2:$HADOOP_HOME/etc/hadoop/
> ```

---

### 3.4 Spark Installation & Environment Configuration
While the primary framework for this project is Hadoop MapReduce, the assignment manual requires manual installation of Spark on all cluster nodes.

1. Download Apache Spark (e.g., Spark 3.5.x pre-built for Hadoop 3) on all machines and extract to `/usr/local/spark`.
2. Add Spark env variables to `~/.bashrc`:
   ```bash
   export SPARK_HOME=/usr/local/spark
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   ```
3. Copy Spark configuration template:
   ```bash
   cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
   ```
4. Edit `$SPARK_HOME/conf/spark-env.sh` and specify variables:
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export SPARK_MASTER_HOST='master-node'
   export SPARK_LOCAL_IP='192.168.1.100'  # Node's local IP address
   ```
5. Edit `$SPARK_HOME/conf/workers` and add:
   ```text
   worker-node1
   worker-node2
   ```

---

## 4. How to Run & Verify

### 4.1 Running Locally (Standalone Mode Simulation)
You can test the entire pipeline locally without setting up HDFS or YARN. Standalone Hadoop uses your local file system.

1. Ensure **Maven** and **Java** are installed and configured.
2. Run the provided script:
   - **On Windows**: Double click [run_local.bat](file:///d:/8th/pdc/project/run_local.bat) or run in CMD:
     ```cmd
     run_local.bat
     ```
   - **On Unix/Linux**: Run:
     ```bash
     chmod +x run_local.sh
     ./run_local.sh
     ```
3. The scripts automatically compile the Java code, generate a 10-year weather CSV dataset (`sample_data.csv`), execute Stage 1 and Stage 2 MapReduce jobs, and print the results to the console.

### 4.2 Running on the Physical Distributed Cluster

#### Step 1: Start Hadoop Daemons
Only on the **Master Node**:
1. If starting HDFS for the first time, format the NameNode:
   ```bash
   hdfs namenode -format
   ```
2. Start HDFS daemons:
   ```bash
   start-dfs.sh
   ```
3. Start YARN resource managers:
   ```bash
   start-yarn.sh
   ```
4. Verify daemons are running on the master using `jps`. You should see `NameNode`, `SecondaryNameNode`, and `ResourceManager`. On workers, running `jps` should show `DataNode` and `NodeManager`.

#### Step 2: Upload Data to HDFS
1. Create a data directory in HDFS:
   ```bash
   hdfs dfs -mkdir -p /weather/input
   ```
2. Upload the `sample_data.csv` dataset:
   ```bash
   hdfs dfs -put sample_data.csv /weather/input/
   ```

#### Step 3: Compile and Package the MapReduce JAR
1. Build the fat JAR on the Master node:
   ```bash
   mvn clean package
   ```
   This generates `target/weather-anomaly-detector-1.0-SNAPSHOT.jar`.

#### Step 4: Run the Job on the Cluster
1. Submit the MapReduce job to YARN:
   ```bash
   hadoop jar target/weather-anomaly-detector-1.0-SNAPSHOT.jar \
       /weather/input \
       /weather/stage1_output \
       /weather/stage2_output
   ```
2. Monitor progress via the Hadoop Resource Manager Web UI: `http://master-node:8088`.

#### Step 5: Check Outputs
1. Check intermediate baseline statistics (Mean & Sigma):
   ```bash
   hdfs dfs -cat /weather/stage1_output/part-r-00000 | head -n 20
   ```
2. Check the list of anomalies:
   ```bash
   hdfs dfs -cat /weather/stage2_output/part-m-00000 | head -n 20
   ```

---

## 5. Understanding the Output

### 5.1 How to Run Locally (No Maven Required)

Hadoop is already sufficient to run the pre-built JAR. No Maven needed:

```bash
bash run_local.sh
```

The script will automatically generate `sample_data.csv`, run both MapReduce stages, and print results.

---

### 5.2 The Three Weather Stations

The sample dataset covers 10 years of daily temperature data for three US airports:

| Station ID | Airport |
|---|---|
| `72295023174` | LAX — Los Angeles International |
| `72530094846` | ORD — Chicago O'Hare International |
| `74486094789` | JFK — New York John F. Kennedy |

---

### 5.3 Stage 1 Output — Baseline Statistics

```
72295023174    63.05,7.26
72530094846    49.93,21.76
74486094789    53.94,18.36
```

Format: `StationID → Mean Temperature (°F), Sigma (σ)`

- **Mean** = the average daily temperature over the full 10-year period.
- **Sigma (σ)** = how much the temperature normally varies up or down (standard deviation). Chicago has a large σ (±22°F) because it experiences both hot summers and freezing winters. LA has a small σ (±7°F) because its climate is mild year-round.

---

### 5.4 Stage 2 Output — Detected Anomalies

A day is flagged as an **anomaly** when its temperature deviates by more than **3σ** from the station's mean — statistically, this happens less than 0.3% of the time under normal conditions.

```
72295023174,2018-07-30,85.10,63.05,7.26,3.04x
72530094846,2022-01-20,-45.00,49.93,21.76,4.36x
72295023174,2023-12-25,25.00,63.05,7.26,5.24x
72295023174,2024-02-05,102.00,63.05,7.26,5.36x
74486094789,2025-08-10,120.00,53.94,18.36,3.60x
```

Format: `StationID, Date, ActualTemp, MeanTemp, Sigma, DeviationMultiple`

| Station | Date | Temp | What it means |
|---|---|---|---|
| LAX | 2018-07-30 | 85.1°F | Unusually hot day for LA — 3.04× beyond normal |
| Chicago | 2022-01-20 | -45.0°F | Extreme cold — 4.36× beyond normal (polar vortex event) |
| LAX | 2023-12-25 | 25.0°F | Extreme cold for LA — 5.24× beyond normal |
| LAX | 2024-02-05 | 102.0°F | Extreme heat for LA — 5.36× beyond normal |
| JFK | 2025-08-10 | 120.0°F | Extreme heat at JFK — 3.60× beyond normal |

The higher the deviation multiple (e.g. `5.36x`), the more extreme the weather event.

---

### 5.5 About the Log Lines

The `INFO mapreduce.Job: ...` lines printed during execution are Hadoop's internal progress logs. The key lines to look for are:

```
map 100% reduce 100%   ← Stage 1 complete
map 100% reduce 0%     ← Stage 2 complete (map-only, no reducer needed)
Job completed successfully
```

Everything else in the logs can be ignored during normal use.

---

## 6. Cross-Group Portability Testing Guide

The **Portability (Transfer) - 40%** grading component requires another group to run your code on their cluster. Follow this guide to verify portability.

### 5.1 Run the Anomaly Filter for a Specific Year
To demonstrate portability, another group can find anomalies in a specific year using the logic. Pass the target year via the `-Dtarget.year` JVM property:

```bash
hadoop jar target/weather-anomaly-detector-1.0-SNAPSHOT.jar \
    -Dtarget.year=2025 \
    /weather/input \
    /weather/stage1_output \
    /weather/stage2_output_2025
```

This will run the same MapReduce binary, but only output anomaly records that occurred in **2025**.

### 5.2 Test with Custom Input Data
Another group can test with their own GSOD-compliant dataset by copying it to HDFS and referencing the path in the input argument:

```bash
hadoop jar target/weather-anomaly-detector-1.0-SNAPSHOT.jar \
    /their_custom_input_dir \
    /weather/stage1_output_custom \
    /weather/stage2_output_custom
```
The Stage 2 Map mapper output file uses the standard CSV-like representation:
`STATION,DATE,TEMP,MEAN,SIGMA,DEVIATION_SIGMAS`

---

## 6. Project Directory Structure

- [pom.xml](file:///d:/8th/pdc/project/pom.xml): Maven project file with dependencies and shader packaging.
- [generate_sample_data.py](file:///d:/8th/pdc/project/generate_sample_data.py): Python data generator for synthetic testing.
- [run_local.bat](file:///d:/8th/pdc/project/run_local.bat): Windows local simulation runner.
- [run_local.sh](file:///d:/8th/pdc/project/run_local.sh): Bash local simulation runner.
- **Java Source Files**:
  - [DoubleSummaryWritable.java](file:///d:/8th/pdc/project/src/main/java/com/pdc/weather/DoubleSummaryWritable.java): Writable containing sums, squared sums, and count to optimize MapReduce via Combiners.
  - [StatsCalculationJob.java](file:///d:/8th/pdc/project/src/main/java/com/pdc/weather/StatsCalculationJob.java): Maps and reduces temperatures to construct the station baseline stats.
  - [AnomalyDetectionJob.java](file:///d:/8th/pdc/project/src/main/java/com/pdc/weather/AnomalyDetectionJob.java): Map-only job checking temperatures against the cached baselines.
  - [WeatherDriver.java](file:///d:/8th/pdc/project/src/main/java/com/pdc/weather/WeatherDriver.java): The program execution entry point orchestrating Stage 1 and Stage 2.

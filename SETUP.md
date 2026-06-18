# Setup Guide — Weather Anomaly Detection

**Platform:** macOS — Intel Mac (2019 or later)
**Hadoop Version:** Apache Hadoop 3.5.0
**Java Version:** OpenJDK 17

> This guide is for **Intel Macs**. If you have an Apple Silicon Mac (M1/M2/M3) the steps are slightly different and this guide does NOT apply.

---

## Before You Start — What Do You Actually Need?

| Goal | Tools Required | Steps to Follow |
|---|---|---|
| **Run on local machine** ✅ (recommended) | Java 17, Hadoop, Python 3 | Steps 1–4, then Steps 9–11 |
| Run on HDFS/YARN (full cluster mode) | All of the above + SSH setup | All steps 1–11 |

> **Maven is NOT required.** The pre-built JAR is already included in `target/`.
> **Spark is NOT required.** This project uses only Hadoop MapReduce.

**Estimated time (local mode only):** 15–20 minutes

---

## What You Will Install

| Tool | Purpose | Required for local mode? |
|---|---|---|
| Homebrew | Package manager — installs everything else | Yes |
| Java 17 (OpenJDK) | Required to run Hadoop and the MapReduce jobs | Yes |
| Apache Hadoop 3.5.0 | Runs the MapReduce pipeline | Yes |
| Python 3 | Generates the sample weather dataset | Yes |
| SSH setup + HDFS format | Needed only for full cluster/YARN mode | **No — skip for local** |

**Estimated time (full setup):** 30–45 minutes (depending on internet speed)

---

## Step 1 — Open Terminal

All commands in this guide are typed into the Terminal application.

**How to open Terminal:**
1. Press `⌘ Command + Space` to open Spotlight Search
2. Type **Terminal** and press Enter

---

## Step 2 — Install Homebrew

Homebrew is a free tool that makes installing software on macOS easy — like an App Store for developers.

First check if it is already installed:
```bash
brew --version
```

If you see a version number like `Homebrew 6.0.2`, it is already installed — **skip to Step 3**.

If NOT installed, paste this and press Enter:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

> It will ask for your Mac login password. Type it in (it won't appear on screen — that is normal) and press Enter.

Verify after installation:
```bash
brew --version
```

---

## Step 3 — Install Java 17

Hadoop requires Java. Install Java 17 using Homebrew:

> **Note:** Do not install a higher Java version — it can cause errors with Hadoop.

```bash
brew install openjdk@17
```

After installation, add Java to your system PATH. Run these **three commands one by one**:

```bash
sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
echo 'export JAVA_HOME=/usr/local/opt/openjdk@17' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
```

> If you use **Bash** instead of Zsh, replace `~/.zshrc` with `~/.bash_profile` in both `echo` commands.

Reload your terminal settings:
```bash
source ~/.zshrc
```

Verify Java is installed correctly:
```bash
java -version
# Expected: openjdk version "17.x.x" ...
```

---

## Step 4 — Install Apache Hadoop

Install Hadoop using Homebrew:
```bash
brew install hadoop
```

Find where Hadoop was installed:
```bash
brew info hadoop
# Installed at: /usr/local/opt/hadoop/libexec
```

---

## Steps 5–8 — Hadoop Configuration, SSH & HDFS

> **For local machine use — you can skip Steps 5 through 8 entirely.**
> The `run_local.sh` script overrides Hadoop settings at runtime to use your local filesystem directly, so no HDFS or YARN configuration is needed.
>
> Only follow Steps 5–8 if you want to run the project on a real HDFS/YARN cluster.

---

## Step 5 — Configure Hadoop

Hadoop needs five configuration files. Navigate to the config folder:
```bash
cd /usr/local/opt/hadoop/libexec/etc/hadoop
```

Open in Finder to confirm you are in the right place:
```bash
open .
```

---

### File 1 — `hadoop-env.sh`
Tells Hadoop where Java is installed.

```bash
nano hadoop-env.sh
```

Scroll to the bottom and add this line:
```bash
export JAVA_HOME=/usr/local/opt/openjdk@17
```

Save and exit: press `Ctrl + X`, then `Y`, then `Enter`.

---

### File 2 — `core-site.xml`
Sets the default address of the Hadoop file system.

```bash
nano core-site.xml
```

Replace everything between `<configuration>` and `</configuration>` with:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

Save and exit: `Ctrl + X` → `Y` → `Enter`.

---

### File 3 — `hdfs-site.xml`
Sets the replication factor to 1 (single machine).

```bash
nano hdfs-site.xml
```

Add inside `<configuration>`:
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

Save and exit: `Ctrl + X` → `Y` → `Enter`.

---

### File 4 — `mapred-site.xml`
Tells Hadoop to use YARN for running jobs.

```bash
nano mapred-site.xml
```

Add inside `<configuration>`:
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

Save and exit: `Ctrl + X` → `Y` → `Enter`.

---

### File 5 — `yarn-site.xml`
Enables the shuffle service needed by MapReduce.

```bash
nano yarn-site.xml
```

Add inside `<configuration>`:
```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

Save and exit: `Ctrl + X` → `Y` → `Enter`.

---

## Step 6 — Enable SSH (Passwordless Login)

Hadoop uses SSH to communicate with itself. You need to enable it and allow passwordless login.

### Enable Remote Login
1. Open **System Preferences**
2. Click **Sharing**
3. Check the box next to **Remote Login** to enable it

### Set Up Passwordless SSH

Run these commands **one by one** in Terminal:
```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Test that SSH works without a password:
```bash
ssh localhost
```

> If it asks `Are you sure you want to continue connecting?` — type `yes` and press Enter. After that it should connect with no password prompt.

Type `exit` to close the SSH session:
```bash
exit
```

---

## Step 7 — Format HDFS & Start Hadoop

### Format the NameNode
This is like formatting a new hard drive. **Do this only once:**
```bash
hdfs namenode -format
```

> **Warning:** Never run this command again later — it will erase all your Hadoop data.

### Start All Hadoop Services
```bash
start-all.sh
```

### Verify All Services Are Running
```bash
jps
```

You should see these **5 services** listed:

| Service | What it does |
|---|---|
| NameNode | Manages HDFS file metadata |
| DataNode | Stores actual data blocks |
| ResourceManager | Manages computing resources |
| NodeManager | Manages resources on this node |
| SecondaryNameNode | Backs up NameNode metadata |

---

## Step 8 — Open Hadoop Web Interface

Open these URLs in any browser (Chrome, Safari, Firefox) to confirm Hadoop is running:

- **HDFS Dashboard:** [http://localhost:9870](http://localhost:9870)
- **YARN Dashboard:** [http://localhost:8088](http://localhost:8088)

> If either page does not load, check `jps` to see which service is missing, then run `start-all.sh` again.

---

## Step 9 — Install Python 3

Python is used to generate the sample weather dataset.

Check if already installed:
```bash
python3 --version
```

If not installed:
```bash
brew install python3
```

---

## Step 10 — Download / Open the Project

If you received the project as a ZIP:
```bash
unzip pdc_project.zip
cd pdc_project
```

Your project folder should look like this:
```
pdc_project/
├── run_local.sh                 ← macOS/Linux runner
├── run_local.bat                ← Windows runner
├── generate_sample_data.py      ← Dataset generator
├── sample_data.csv              ← Pre-generated dataset
├── SETUP.md                     ← This file
├── README.md                    ← Full project documentation
├── pom.xml                      ← Maven build file
└── target/
    └── weather-anomaly-detector-1.0-SNAPSHOT.jar  ← Pre-built JAR
```

---

## Step 11 — Run the Project

Go into the project folder and run:
```bash
cd pdc_project
bash run_local.sh
```

---

### What `run_local.sh` Does Internally — Step by Step

You don't need to understand this to run it, but here is exactly what happens when you execute the script:

**1. Checks Hadoop is installed**
If `hadoop` command is not found it stops immediately with an error.

**2. Checks the pre-built JAR exists**
Looks for `target/weather-anomaly-detector-1.0-SNAPSHOT.jar`. This JAR is already included — no compilation needed.

**3. Generates the dataset (only if `sample_data.csv` is missing)**
Runs `python3 generate_sample_data.py` to create ~10,960 rows of synthetic weather data across 3 airports over 10 years (2016–2025). If the file already exists, this step is skipped.

**4. Deletes old output folders**
Removes `stage1_output/` and `stage2_output/` from any previous run so Hadoop does not complain about existing directories.

**5. Runs the MapReduce pipeline**
```bash
hadoop jar target/weather-anomaly-detector-1.0-SNAPSHOT.jar \
    -Dfs.defaultFS=file:///            ← use local files, not HDFS
    -Dmapreduce.framework.name=local   ← run locally, not on YARN
    sample_data.csv stage1_output stage2_output
```
This single command runs both Stage 1 (baseline stats) and Stage 2 (anomaly detection) on your local machine with no cluster needed.

**6. Prints Stage 1 results**
Reads and prints `stage1_output/part-r-00000` — the mean temperature and sigma for each station.

**7. Prints Stage 2 results**
Reads and prints `stage2_output/part-m-00000` — every day that was flagged as an extreme weather anomaly.

---

## Expected Output

### Stage 1 — Baseline Statistics
```
72295023174    63.05,7.26      ← LAX  — avg 63°F, varies ±7°F
72530094846    49.93,21.76     ← ORD  — avg 50°F, varies ±22°F
74486094789    53.94,18.36     ← JFK  — avg 54°F, varies ±18°F
```

### Stage 2 — Detected Anomalies
```
72295023174,2018-07-30,85.10,63.05,7.26,3.04x   ← Unusually hot day at LAX
72530094846,2022-01-20,-45.00,49.93,21.76,4.36x  ← Extreme cold in Chicago
72295023174,2023-12-25,25.00,63.05,7.26,5.24x    ← Extreme cold at LAX
72295023174,2024-02-05,102.00,63.05,7.26,5.36x   ← Extreme heat at LAX
74486094789,2025-08-10,120.00,53.94,18.36,3.60x  ← Extreme heat at JFK
```

**Columns:** `StationID, Date, ActualTemp(°F), MeanTemp, Sigma, DeviationMultiple`

Output files are also saved locally:
- `stage1_output/part-r-00000` — baseline stats
- `stage2_output/part-m-00000` — anomaly records

---

## Stopping Hadoop

When you are done, always stop Hadoop properly:
```bash
stop-all.sh
```

To start Hadoop again in a future session:
```bash
start-all.sh
```

> You do **NOT** need to format the NameNode again. Only run `start-all.sh` to restart.

---

## Quick Reference — Hadoop Commands

| Command | What it does |
|---|---|
| `start-all.sh` | Start all Hadoop services |
| `stop-all.sh` | Stop all Hadoop services |
| `jps` | Check which services are running |
| `hdfs dfs -ls /` | List files in HDFS root |
| `hdfs dfs -mkdir /folder` | Create a folder in HDFS |
| `hdfs dfs -put file.txt /path/` | Upload a file to HDFS |
| `hdfs dfs -cat /path/file.txt` | Read a file from HDFS |
| `hdfs dfs -rm /path/file.txt` | Delete a file from HDFS |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `brew command not found` | Homebrew is not installed — re-run the install command from Step 2 |
| `JAVA_HOME not found` | Make sure you added the `export` lines to `~/.zshrc` and ran `source ~/.zshrc` |
| `Java version mismatch` | Only install `openjdk@17` — higher versions can break Hadoop |
| SSH connection refused | Go to System Preferences → Sharing and make sure **Remote Login** is ON |
| Only 3–4 services in `jps` | Run `stop-all.sh`, wait 10 seconds, then run `start-all.sh` again |
| Web UI not loading | Run `jps` — if NameNode is missing, run `start-dfs.sh` |
| Port 9000 already in use | Run `kill -9 $(lsof -ti :9000)` then run `start-all.sh` |
| `JAR not found` | The pre-built JAR should be in `target/`. If missing, rebuild: `brew install maven && mvn clean package` |
| Permission denied on `run_local.sh` | Run `chmod +x run_local.sh` first |

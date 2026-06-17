import os

def configure():
    hadoop_conf_dir = "/usr/local/hadoop/etc/hadoop"
    
    if not os.path.exists(hadoop_conf_dir):
        print(f"[ERROR] Hadoop configuration directory not found at {hadoop_conf_dir}.")
        print("Please make sure Hadoop is installed and extracted at /usr/local/hadoop first.")
        return

    print("Configuring Hadoop configuration files...")

    # 1. hadoop-env.sh
    env_path = os.path.join(hadoop_conf_dir, "hadoop-env.sh")
    with open(env_path, "r") as f:
        content = f.read()
    if "export JAVA_HOME=" not in content or "# export JAVA_HOME=" in content:
        content += "\nexport JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64\n"
        with open(env_path, "w") as f:
            f.write(content)
        print(" -> Configured hadoop-env.sh (set JAVA_HOME)")

    # 2. core-site.xml
    core_site_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
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
"""
    with open(os.path.join(hadoop_conf_dir, "core-site.xml"), "w") as f:
        f.write(core_site_content)
    print(" -> Configured core-site.xml")

    # 3. hdfs-site.xml
    hdfs_site_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
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
"""
    with open(os.path.join(hadoop_conf_dir, "hdfs-site.xml"), "w") as f:
        f.write(hdfs_site_content)
    print(" -> Configured hdfs-site.xml")

    # 4. mapred-site.xml
    mapred_site_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
"""
    with open(os.path.join(hadoop_conf_dir, "mapred-site.xml"), "w") as f:
        f.write(mapred_site_content)
    print(" -> Configured mapred-site.xml")

    # 5. yarn-site.xml
    yarn_site_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
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
"""
    with open(os.path.join(hadoop_conf_dir, "yarn-site.xml"), "w") as f:
        f.write(yarn_site_content)
    print(" -> Configured yarn-site.xml")

    # 6. workers
    with open(os.path.join(hadoop_conf_dir, "workers"), "w") as f:
        f.write("worker-node1\n")
    print(" -> Configured workers file")

    print("\n[SUCCESS] Hadoop configuration files have been successfully configured!")

if __name__ == "__main__":
    configure()

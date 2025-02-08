# Word Count program  
This program is adapted from the source code below

https://codewitharjun.com/word-count-example-using-hadoop-and-java/

https://medium.com/@akbains36/hadoop-map-reduce-program-to-find-top-ten-trending-hashtags-on-twitter-517bf7b70e48

## Installation
Hadoop requires JAVA JDK 8

Follow the instruction from this guide to install Hadoop on MacOS devices

https://levelup.gitconnected.com/install-hadoop-on-macos-m1-m2-6f6a01820cc9

## Launch Hadoop

Once you have Hadoop installed and configured, direct to the Hadoop directory hit the following command to refresh the environment variable. 

```bash
source ~/.zprofile 
```

Now Format the HDFS namenode.

```bash
hdfs namenode -format
```
Hit jps to check if all nodes are running. Pay special attention to datanodes and namenodes. If you don't see datanodes, stop all, go to Hadoop folder, delete the datanode folder and start again from Launch. 


If everything is good, start all of Hadoop
```bash
start-all.sh
```
Now go to http://localhost:9870

To create the directory and add files or directories to existing director
```` bash
hadoop fs -mkdir /input1
hadoop fs -put unique_terms.txt /input1
hadoop fs -put Wiki_full_5000 /input1
````

## Running program
Once you have the program ready, compile the files and create a jar file 

Use the following command in terminal to run MapReduce job

```` bash
hadoop jar target/<jar-file-name>.jar org.<your-org-name>.<java-file> /<link to input file or directory> /<link to output>
````

Example: 

```` bash
hadoop jar target/WordCount-1.0-SNAPSHOT.jar org.hadoopmapreduce.WC_Runner /input2/Quyen_Tran_CS.txt /outputQuyen
````
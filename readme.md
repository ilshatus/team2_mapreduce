Change the following line to specify class to execute in the **pom.xml**
```
<exec.mainClass>[Class to execute]</exec.mainClass>
```

Write this command in the Intellij IDEA terminal to compile executable jar
```
mvn clean compile assembly:single
```

To access cluster (or use Putty)
```
ssh team2@10.90.138.32
```

To run the app
```
/hadoop/bin/hadoop jar [app_name].jar
```

If the directory already exists:
```
/hadoop/bin/hadoop fs -rm -r [dir_name]
```

To get file from hdfs:
```
/hadoop/bin/hdfs dfs -get [path_to_file]
```
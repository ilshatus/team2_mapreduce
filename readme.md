Write this command to compile executable jar
```
mvn clean compile assembly:single
```

To access cluster (or use Putty)
```
ssh team2@10.90.138.32
```

To see usage help
```
/hadoop/bin/hadoop jar search_engine.jar 
```

To get file from hdfs:
```
/hadoop/bin/hdfs dfs -get [path_to_file]
```

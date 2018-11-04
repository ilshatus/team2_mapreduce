Write this command to compile executable jar
```
mvn clean compile assembly:single
```

To access cluster (or use Putty)
```
ssh team2@10.90.138.32
```

To see usage
```
/hadoop/bin/hadoop jar search_engine.jar 
```

```
Usage:	Indexer INPUT_FOLDER [IDF_OUTPUT_FOLDER TF_IDF_OUTPUT_FOLDER]
		Default for IDF_OUTPUT_FOLDER is 'idf_output'
		Default for TF_IDF_OUTPUT_FOLDER is 'tf_idf_output'

Usage:	Query [TF_IDF_INPUT_FOLDER RESULT_FOLDER] TOP QUERY_TEXT
		Default for TF_IDF_INPUT_FOLDER is 'tf_idf_output'
		Default for RESULT_FOLDER is 'result'
```

To get file from hdfs:
```
/hadoop/bin/hdfs dfs -get [path_to_file]
```

# large-row


### Test data
Raw data: https://github.com/openai/gpt-2-output-dataset

```
df.printSchema()

root
 |-- id: long (nullable = true)
 |-- json_0: struct (nullable = true)
 |    |-- ended: boolean (nullable = true)
 |    |-- length: long (nullable = true)
 |    |-- text: string (nullable = true)
 |-- json_1: struct (nullable = true)
 |    |-- ended: boolean (nullable = true)
 |    |-- length: long (nullable = true)
 |    |-- text: string (nullable = true)
 |-- json_2: struct (nullable = true)
 |    |-- ended: boolean (nullable = true)
 |    |-- length: long (nullable = true)
 |    |-- text: string (nullable = true)
 |-- ...
 |    |-- ...


df.show()

+------+--------------------+--------------------+--------------------+--------+
|    id|              json_0|              json_1|              json_2|     ...|
+------+--------------------+--------------------+--------------------+--------+
|255000|{true, 134, Is th...|{true, 134, Is th...|{true, 134, Is th...|     ...|
|255001|{true, 713, Clint...|{true, 713, Clint...|{true, 713, Clint...|     ...|
|255002|{true, 173, House...|{true, 173, House...|{true, 173, House...|     ...|
+------+--------------------+--------------------+--------------------+--------+
```

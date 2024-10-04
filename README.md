# large-row


### Test data
Raw data: https://github.com/openai/gpt-2-output-dataset

generating script: create_large_row_openai.py

```
> df.printSchema()

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


> df.show()

+------+--------------------+--------------------+--------------------+--------+
|    id|              json_0|              json_1|              json_2|     ...|
+------+--------------------+--------------------+--------------------+--------+
|255000|{true, 134, Is th...|{true, 134, Is th...|{true, 134, Is th...|     ...|
|255001|{true, 713, Clint...|{true, 713, Clint...|{true, 713, Clint...|     ...|
|255002|{true, 173, House...|{true, 173, House...|{true, 173, House...|     ...|
+------+--------------------+--------------------+--------------------+--------+
```

### Test queries
``` python
# Read data
df = spark.read.parquet(f"data/openai/parquet/10_cols_1_mb.parquet")
df.createOrReplaceTempView("T")
```

```python
# Q1. get the length of structure's long string
spark.sql("SELECT length(json_0['text']) FROM T").collect()

"""
Output: 
	[Row(length(json_0.text)=1048576),
	Row(length(json_0.text)=1048576),
	Row(length(json_0.text)=1048576),
	...]
 """
```

```python
# Q2. find word in structure's long string
spark.sql("SELECT id, json_0 FROM T WHERE contains(json_0['text'], 'House')").show()

"""
Output:
	+------+--------------------+
	|    id|              json_0|
	+------+--------------------+
	|255002|{true, 173, House...|
	+------+--------------------+
"""
```

```python
# Q3. lowercase the structure's long string
spark.sql("SELECT lower(json_0['text']) FROM T").show()

"""
Output:
	+--------------------+
	|  lower(json_0.text)|
	+--------------------+
	|is this restauran...|
	|clinton talks abo...|
	|house majority wh...|
	|        ...         |
	+--------------------+
"""
```

```python
# Q4. UDF: count word frequency in structure's long string
def word_count(text):
    word_count = {}
    for word in text.split():
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    return word_count


spark.udf.register("word_count", word_count)
spark.sql("SELECT word_count(json_0['text']) FROM T").collect()

"""
Output:
	[Row(word_count(json_0.text)='{specialty=2048, be=2048, hidden=1024, ...}'),
	Row(word_count(json_0.text)='{told=1024, crowd=2048, allowed=1024, ...}'),
	Row(word_count(json_0.text)='{practice=1024, been=2048, statement:=1024, ...}'),
	...]
"""
```
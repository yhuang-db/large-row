# large-row


### Test data
Raw data: https://github.com/openai/gpt-2-output-dataset

Generating script: ```generate_string_table.py```

```sh
# generate test parquet file with 100 rows, 10 columns, where each cell is a 10 MB string
python generate_string_table.py -r 100 -c 10 -s 10
```
Generated data: ```large_string_100row_10col_10m.parquet```

| string_0                                               | string_1                                      | string_2                                                 | ... |
| ------------------------------------------------------ | --------------------------------------------- | -------------------------------------------------------- | --- |
| Is this restaurant family-friendly ? Yes No Unsure...  | House Majority Whip Steve Scalise has been... | BY JENNIE MCNULTY\n\nLesbian.com\n\nYou know...          | ... |
| Clinton talks about her time of 'reflection' during... | Insight Course: Lesson 14\n\nControl...       | The Buddha's Teaching As It Is\n\nIn the fall of 1979... | ... |
| ...                                                    | ...                                           | ...                                                      | ... |


### Test queries
Test pyspark file: ```udf_upper.py```

```Python
spark = SparkSession.builder.appName("benchmark").getOrCreate()

def udf_upper(text):
    return text.upper()
spark.udf.register("udf_upper", udf_upper)

df = spark.read.parquet("large_string_100row_10col_10m.parquet")
df.createOrReplaceTempView("T")
df_upper = spark.sql("SELECT udf_upper(string_1) FROM T")
df_upper.write.parquet("output.parquet")
spark.stop()
```

Test spark-submit commend line: according to https://github.com/dongjoon-hyun/spark/blob/master/.github/workflows/benchmark.yml
```sh
../bin/spark-submit --driver-memory 6g udf_upper.py
```
---

|                  | builtin_upper      | udf_upper          | builtin_length     | udf_length         |
| ---------------- | ------------------ | ------------------ | ------------------ | ------------------ |
| 100row_10col_1m  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 100row_10col_5m  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 100row_10col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 100row_10col_20m | :x:                | :x:                | :white_check_mark: | :x:                |
| 100row_10col_30m | :x:                | :x:                | :x:                | :x:                |

|                 | builtin_upper      | udf_upper          | builtin_length     | udf_length         |
| --------------- | ------------------ | ------------------ | ------------------ | ------------------ |
| 50row_1col_10m  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| 60row_1col_10m  | :white_check_mark: | :x:                | :white_check_mark: | :white_check_mark: |
| 70row_1col_10m  | :white_check_mark: | :x:                | :white_check_mark: | :white_check_mark: |
| 80row_1col_10m  | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 90row_1col_10m  | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 100row_1col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 125row_1col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 150row_1col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 175row_1col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 200row_1col_10m | :white_check_mark: | :x:                | :white_check_mark: | :x:                |
| 250row_1col_10m | :x:                | :x:                | :x:                | :x:                |
| 300row_1col_10m | :x:                | :x:                | :x:                | :x:                |

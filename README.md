# large-row


### Test Data
Raw data: https://github.com/openai/gpt-2-output-dataset

Generating script: ```generate_string_table.py```

```sh
# generate test parquet file with 100 rows, 10 columns, where each cell is a 10 MB string
python generate_string_table.py -r 100 -c 10 -s 10
```
Generated data: ```large_string_100row_10col_10m.parquet```

| string_0                                                | string_1                                       | string_2                                                  | ... |
| ------------------------------------------------------- | ---------------------------------------------- | --------------------------------------------------------- | --- |
| Is this restaurant family-friendly ? Yes No Unsure ...  | House Majority Whip Steve Scalise has been ... | BY JENNIE MCNULTY\n\nLesbian.com\n\nYou know ...          | ... |
| Clinton talks about her time of 'reflection' during ... | Insight Course: Lesson 14\n\nControl ...       | The Buddha's Teaching As It Is\n\nIn the fall of 1979 ... | ... |
| ...                                                     | ...                                            | ...                                                       | ... |


### Test Jobs
Test scala file: ```scala/src/main/scala/BuiltinUpper.scala```

```Scala
import org.apache.spark.sql.SparkSession

object BuiltinUpper {
  def main(args: Array[String]): Unit = {
    // dataset
    val file = s"data/${dataset}.parquet"
    
    // read parquet file
    val spark = SparkSession.builder.appName("Builtin-Upper").getOrCreate()
    val df = spark.read.parquet(file)
    df.createOrReplaceTempView("T")
    df.printSchema()
    
    // run sql and write to parquet
    val sql_projection = df.columns.map(c => s"UPPER(${c}) as ${c}").mkString(", ")
    val sql = s"SELECT ${sql_projection} FROM T"
    val df_out = spark.sql(sql)
    df_out.write.mode("overwrite").parquet("output/blt_upper.parquet")
    spark.stop()
  }
}

```

Test spark-submit commend line: according to https://github.com/dongjoon-hyun/spark/blob/master/.github/workflows/benchmark.yml
```sh
spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "BuiltinUpper" \
  --master local[1] \
  --driver-memory 6g \
  target/scala-2.13/simple-project_2.13-1.0.jar large_string_100row_10col_10m
```

---

### Result

|                  | builtin_upper      | udf_upper          | builtin_length | udf_length |
| ---------------- | ------------------ | ------------------ | -------------- | ---------- |
| 100row_10col_1m  | :white_check_mark: | :white_check_mark: |                |            |
| 100row_10col_5m  | :x:                | :x:                |                |            |
| 100row_10col_10m | :x:                | :x:                |                |            |


|                 | builtin_upper      | udf_upper          | builtin_length | udf_length |
| --------------- | ------------------ | ------------------ | -------------- | ---------- |
| 1row_1col_500m  | :white_check_mark: | :white_check_mark: |                |            |
| 1row_1col_1000m | :x:                | :x:                |                |            |


|                 | builtin_upper      | udf_upper          | builtin_length | udf_length |
| --------------- | ------------------ | ------------------ | -------------- | ---------- |
| 200row_1col_10m | :white_check_mark: | :white_check_mark: |                |            |
| 250row_1col_10m | :x:                | :x:                |                |            |


|                 | builtin_upper      | udf_upper | builtin_length | udf_length |
| --------------- | ------------------ | --------- | -------------- | ---------- |
| 1row_50col_10m  | :white_check_mark: | :x:       |                |            |
| 1row_100col_10m | :x:                | :x:       |                |            |

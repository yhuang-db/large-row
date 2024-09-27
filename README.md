# large-row


### Test data

| uid (int) | name (string) | col_0 (array<double>) | col_1 (array<double>) | ... |
| --------- | ------------- | --------------------- | --------------------- | --- |
| 1         | Alice         | [0.453, 0.897, ...]   | [0.462, 0.178, ...]   | ... |

#### Test files
```bash
// to generate test files to folder "data/"
python gen_large_row_tables.py
```

`{m}_cols_{n}_mb.parquet`: a table with **1 row**, ***m* columns** of large cell, where each cell is an array<double> of ***n* MB** size. 

1_kb array: 1024 / 8 * 1 = 128 double

1_mb array: 1024 * 1024 / 8 * 1 = 131,072 double

10_mb array: 1024 * 1024 / 8 * 10 = 1,310,720 double

...

> parquet or csv file?
>
> different in file size
> 
> CSV parser buffer size: https://javadoc.io/doc/com.univocity/univocity-parsers/latest/com/univocity/parsers/common/CommonParserSettings.html#setInputBufferSize-int-


### Test queries

``` python
# Q1. whole row collect
df.collect()
```

``` python
# Q2. single column max
df.select(array_max("col_0")).collect()
```

``` python
# Q3. single column sort
df.select(array_sort("col_0").alias('sorted_col_0')).collect()
```

``` python
# Q4. single column aggregation function?
df.select(F.aggregate("col_0", F.lit(0.0), lambda acc, x: acc + x).alias("total")).collect()
```

``` python
# Q5. single column UDF
spark.sql("select get_second_max(col_0) from t").collect()
```

``` python
# Q6. single column UDAF (need data with more rows)
```

``` python
# Q7. multi-column built-in function
```

``` python
# Q8. multi-column UDF
```

``` python
# Q9. multi-column UDAF
```

``` python
# Q10. UDF apply on all columns?
```

---

## Test result

```bash
// example to run a test
python python benchmark.py -t 1_cols_1_mb
```

##### Spark session config

##### Survival test

| Test file       |     Read file      |         Q1         |         Q2         |         Q3         |         Q4         |         Q5         |  ...  |
| --------------- | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: | :----------------: | :---: |
| 1_cols_1_kb     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 1_cols_1_mb     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 1_cols_10_mb    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 1_cols_100_mb   | :white_check_mark: | :white_check_mark: | :white_check_mark: |        :x:         |                    |                    |  ...  |
| 10_cols_1_kb    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 10_cols_1_mb    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 10_cols_10_mb   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |  ...  |
| 10_cols_100_mb  | :white_check_mark: |                    |                    |                    |                    |                    |  ...  |
| 100_cols_1_kb   |                    |                    |                    |                    |                    |                    |  ...  |
| 100_cols_1_mb   |                    |                    |                    |                    |                    |                    |  ...  |
| 100_cols_10_mb  |                    |                    |                    |                    |                    |                    |  ...  |
| 100_cols_100_mb |                    |                    |                    |                    |                    |                    |  ...  |

##### Performance test

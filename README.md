# large-row


### Test data
Raw data: https://github.com/openai/gpt-2-output-dataset

generating script: create_data.py

```
large_string_n_row.parquet
n_row: 5 n_col: 12

root
 |-- id: long (nullable = true)
 |-- 1m_string: string (nullable = true)
 |-- 10m_string: string (nullable = true)
 |-- 20m_string: string (nullable = true)
 |-- 30m_string: string (nullable = true)
 |-- 40m_string: string (nullable = true)
 |-- 50m_string: string (nullable = true)
 |-- 60m_string: string (nullable = true)
 |-- 70m_string: string (nullable = true)
 |-- 80m_string: string (nullable = true)
 |-- 90m_string: string (nullable = true)
 |-- 100m_string: string (nullable = true)
```

---

For 10m_string

|        | 10_col | 20_col | 30_col | 40_col |
| ------ | ------ | ------ | ------ | ------ |
| 10_row | pass   | pass   | pass   | pass   |
| 20_row | Fail   |        |        |        |

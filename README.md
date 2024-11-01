# large-row


### Test data
Raw data: https://github.com/openai/gpt-2-output-dataset

generating script: ```generate_string_table.py```

```sh
# generate test parquet file with 100 rows, 10 columns, each cell of 10 MB
python generate_string_table.py -r 100 -c 10 -s 10
```
Generated data: ```large_string_100row_10col_10m.parquet```

| string_0                                               | string_1                                      | string_2                                                 | ... |
| ------------------------------------------------------ | --------------------------------------------- | -------------------------------------------------------- | --- |
| Is this restaurant family-friendly ? Yes No Unsure...  | House Majority Whip Steve Scalise has been... | BY JENNIE MCNULTY\n\nLesbian.com\n\nYou know...          | ... |
| Clinton talks about her time of 'reflection' during... | Insight Course: Lesson 14\n\nControl...       | The Buddha's Teaching As It Is\n\nIn the fall of 1979... | ... |
| ...                                                    | ...                                           | ...                                                      | ... |



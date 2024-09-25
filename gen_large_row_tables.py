import random
import pandas as pd


def get_random_char_from(string):
    return random.choice(string)


def gen_large_cell(char_seeds, mb):
    char = get_random_char_from(char_seeds)
    return char * 1024 * 1024 * mb


if __name__ == "__main__":
    char_seeds = "abc"

    # create df with 200 1MB col
    df_sm = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            **{f"col_{i}_1m": [gen_large_cell(char_seeds, 1) for _ in range(3)] for i in range(200)},
        }
    )
    df_sm.to_csv("data/data_sm_col.csv", index=False)
    df_sm.to_parquet("data/data_sm_col.parquet", index=False)
    print("Done small col.")

    # create df with 20 10MB col
    df_md = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            **{f"col_{i}_10m": [gen_large_cell(char_seeds, 10) for _ in range(3)] for i in range(20)},
        }
    )
    df_md.to_csv("data/data_md_col.csv", index=False)
    df_md.to_parquet("data/data_md_col.parquet", index=False)
    print("Done medium col.")

    # create df with 2 100MB col
    df_lg = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            **{f"col_{i}_100m": [gen_large_cell(char_seeds, 100) for _ in range(3)] for i in range(2)},
        }
    )
    df_lg.to_csv("data/data_lg_col.csv", index=False)
    df_lg.to_parquet("data/data_lg_col.parquet", index=False)
    print("Done large col.")

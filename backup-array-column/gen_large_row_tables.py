import random
import pandas as pd


def get_random_char_from(string):
    return random.choice(string)


def gen_kb_string(char_seeds, kb):
    char = get_random_char_from(char_seeds)
    return char * 1024 * kb


def gen_mb_string(char_seeds, mb):
    char = get_random_char_from(char_seeds)
    return char * 1024 * 1024 * mb


def gen_random_double():
    return random.random()


def gen_kb_double_array(kb):
    return [gen_random_double() for _ in range(int(1024 * kb / 8))]


def gen_mb_double_array(mb):
    return [gen_random_double() for _ in range(int(1024 * 1024 * mb / 8))]


def gen_kb_cell_df(col_num, kb):
    df = pd.DataFrame(
        {"uid": [1], "name": ["Alice"], **{f"col_{i}": [gen_kb_double_array(kb)] for i in range(col_num)}}
    )
    return df


def gen_mb_cell_df(col_num, mb):
    df = pd.DataFrame(
        {"uid": [1], "name": ["Alice"], **{f"col_{i}": [gen_mb_double_array(mb)] for i in range(col_num)}}
    )
    return df


if __name__ == "__main__":
    # gen kb cell tables
    for col_num in [1, 10, 100]:
        df = gen_kb_cell_df(col_num, 1)
        df.to_csv(f"data/{col_num}_cols_1_kb.csv", index=False)
        df.to_parquet(f"data/{col_num}_cols_1_kb.parquet", index=False)
        print(f"Generated {col_num}_cols_1_kb")

    # gen mb cell tables
    for col_num in [1, 10, 100]:
        for mb in [1, 10, 100]:
            if col_num == 100 and mb == 100:
                continue
            df = gen_mb_cell_df(col_num, mb)
            df.to_csv(f"data/{col_num}_cols_{mb}_mb.csv", index=False)
            df.to_parquet(f"data/{col_num}_cols_{mb}_mb.parquet", index=False)
            print(f"Generated {col_num}_cols_{mb}_mb")

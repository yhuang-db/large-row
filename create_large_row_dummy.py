import pandas as pd


sentence_32byte = "This is a sentence of 32 bytes. "
sentence_32byte_2 = "Another 32-byte sentence it is. "
assert len(sentence_32byte) == 32
assert len(sentence_32byte_2) == 32


def gen_dummy_sentence_kb(kb):
    repeat = kb * 1024 // 32
    return sentence_32byte * repeat


def gen_dummy_sentence_mb(mb):
    return gen_dummy_sentence_kb(mb * 1024)


def gen_kb_cell_df(col_num, kb):
    df = pd.DataFrame(
        {
            "uid": [1],
            **{f"col_{i}": [gen_dummy_sentence_kb(kb)] for i in range(col_num)},
        }
    )
    return df


def gen_mb_cell_df(col_num, mb):
    df = pd.DataFrame(
        {
            "uid": [1],
            **{f"col_{i}": [gen_dummy_sentence_mb(mb)] for i in range(col_num)},
        }
    )
    return df


if __name__ == "__main__":
    # gen kb cell tables
    col_num = 10
    df = gen_kb_cell_df(col_num, 1)
    df.to_parquet(f"data/dummy/{col_num}_cols_1_kb.parquet", index=False)
    print(f"Generated {col_num}_cols_1_kb")

    # gen mb cell tables
    for col_num in [1, 10, 100]:
        for mb in [1, 10, 100]:
            df = gen_mb_cell_df(col_num, mb)
            df.to_parquet(f"data/dummy/{col_num}_cols_{mb}_mb.parquet", index=False)
            print(f"Generated {col_num}_cols_{mb}_mb")

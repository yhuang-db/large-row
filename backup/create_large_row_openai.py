import pandas as pd

df = pd.read_json("data/openai/webtext.test.jsonl", lines=True)
df["json_0"] = df.apply(
    lambda row: {
        "ended": row["ended"],
        "length": row["length"],
        "text": row["text"],
    },
    axis=1,
)
df_raw = df.drop(columns=["ended", "length", "text"])
df_raw = df_raw.head(10)


def extend_json_text_to_length(row, length):
    text = row["json_0"]["text"]
    repeat = length // len(text)
    new_text = text * (repeat + 1)
    row["json_0"]["text"] = new_text[:length]
    return row


def gen_kb_json_df(df, kb, cols):
    target_length = kb * 1024
    df = df.apply(extend_json_text_to_length, length=target_length, axis=1)
    df = pd.concat([df] + [df["json_0"]] * (cols - 1), axis=1)
    df.columns = ["id"] + [f"json_{i}" for i in range(cols)]
    return df


def gen_md_json_df(df, mb, cols):
    return gen_kb_json_df(df, mb * 1024, cols)


if __name__ == "__main__":
    # Generate 1_cols_1_kb.parquet: each row has 1 large cell of 1KB
    df = gen_kb_json_df(df_raw, 1, 1)
    df.to_parquet(f"data/openai/parquet/1_cols_1_kb.parquet", index=False)
    print("Generated 1_cols_1_kb.parquet")

    # Generate m_cols_n_mb.parquet: each row has m large
    # cells of n MB
    for mb in [1, 10, 100]:
        for cols in [1, 10, 100]:
            if mb == 100 and cols == 100:
                break
            df = gen_md_json_df(df_raw, mb, cols)
            df.to_parquet(f"data/openai/parquet/{cols}_cols_{mb}_mb.parquet", index=False)
            print(f"Generated {cols}_cols_{mb}_mb.parquet")

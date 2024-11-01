import argparse
import pandas as pd


def extend_text_to_length(text, length):
    repeat = length // len(text) + 1
    new_text = text * repeat
    return new_text[:length]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="number of rows", type=int)
    parser.add_argument("-c", help="number of columns", type=int)
    parser.add_argument("-s", help="size of string", type=int)
    args = parser.parse_args()
    n_row = args.r
    n_col = args.c
    size = args.s
    print(f"n_row: {n_row}, n_col: {n_col}, size: {size}")

    df = pd.read_json("data/openai/webtext.test.jsonl", lines=True)

    df = df.head(n_row * n_col)
    string_length = 1024 * 1024 * size
    df[f"{size}m_string"] = df["text"].apply(extend_text_to_length, length=string_length)
    df = df.drop(columns=["id", "ended", "length", "text"])
    reshaped = df.values.reshape(n_row, n_col)
    df = pd.DataFrame(reshaped, columns=[f"string_{i+1}" for i in range(n_col)])

    print("Created data")
    df.to_parquet(f"large_string_{n_row}row_{n_col}col_{size}m.parquet", index=False)
    print(f"Generated large_string_{n_row}row_{n_col}col_{size}m.parquet\n")

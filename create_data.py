import pandas as pd


def extend_text_to_length(text, length):
    repeat = length // len(text) + 1
    new_text = text * repeat
    return new_text[:length]


df = pd.read_json("data/openai/webtext.test.jsonl", lines=True)
n_row = 5
df = df.head(n_row)

df["1m_string"] = df["text"].apply(extend_text_to_length, length=1024 * 1024)

df["10m_string"] = df["1m_string"] * 10
df["20m_string"] = df["1m_string"] * 20
df["30m_string"] = df["1m_string"] * 30
df["40m_string"] = df["1m_string"] * 40
df["50m_string"] = df["1m_string"] * 50
df["60m_string"] = df["1m_string"] * 60
df["70m_string"] = df["1m_string"] * 70
df["80m_string"] = df["1m_string"] * 80
df["90m_string"] = df["1m_string"] * 90
df["100m_string"] = df["1m_string"] * 100


# n_col = 40
# for i in range(1, n_col + 1):
#     df[f"10m_string_{i}"] = df["1m_string"] * 10


print("create data")
df = df.drop(columns=["ended", "length", "text"])
df.to_parquet("large_string_5row.parquet", index=False)

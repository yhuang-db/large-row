import pandas as pd


if __name__ == "__main__":
    df = pd.read_json("data/openai/webtext.test.jsonl", lines=True)
    df["text"] = df["text"].apply(lambda x: x.encode("unicode-escape").decode())
    df["text"].to_csv("data/text/webtext.test.txt", index=False, header=False)

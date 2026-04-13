import readchar
import pandas as pd

FILE = "dota_chat_suhaas.csv"
LABELS = {"i": "neutral", "o": "banter", "p": "toxic"}

# Reset labels (run this block once, then comment it out for resume sessions)
# df = pd.read_csv(FILE)
# df["new_label"] = ""
# df.to_csv(FILE, index=False)

# Reload with explicit dtype
df = pd.read_csv(FILE, dtype={"new_label": "string"})
if "new_label" not in df.columns:
    df["new_label"] = pd.Series([""] * len(df), dtype="string")

# Normalize: treat NaN and "" the same
df["new_label"] = df["new_label"].fillna("").astype("string")

todo = df[df["new_label"] == ""].index.tolist()
print(f"{len(todo)} rows remaining")
print("Keys: [i]neutral  [o]banter  [p]toxic  [d]rop  [u]ndo  [q]uit\n")

last = None
i = 0
while i < len(todo):
    idx = todo[i]
    print(f"[{i+1}/{len(todo)}]  {df.at[idx, 'message']!r}")
    key = readchar.readkey().lower()

    if key == "q":
        break

    if key == "u" and last is not None:
        df.at[last, "new_label"] = ""
        print("  undone\n")
        i = max(0, i - 1)
        last = None
        continue

    if key == "d":
        df.at[idx, "new_label"] = "DROP"
        last = idx
        print("  → dropped\n")
        i += 1
        if i % 25 == 0:
            df.to_csv(FILE, index=False)
            print(f"  saved ({i})\n")
        continue

    if key not in LABELS:
        print("  invalid\n")
        continue

    df.at[idx, "new_label"] = LABELS[key]
    last = idx
    print(f"  → {LABELS[key]}\n")
    i += 1

    if i % 25 == 0:
        df.to_csv(FILE, index=False)
        print(f"  saved ({i})\n")

final = df[df["new_label"] != "DROP"].copy()
final.to_csv(FILE, index=False)
print(f"done — {(df['new_label'] == 'DROP').sum()} rows dropped, {len(final)} remaining")
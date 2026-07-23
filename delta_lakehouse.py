"""
Capstone Deliverable 2: Delta Lakehouse (Bronze / Silver / Gold)
Dataset: books.csv (Title, Author, Genre, Height, Publisher)

Uses the `deltalake` package (delta-rs Python bindings) -- a real Delta
Lake implementation, no Spark/JVM/Maven required. Produces real Delta
tables: an actual _delta_log/, real ACID commits, real schema
enforcement, real MERGE.
"""

import shutil
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

RAW_CSV = "./data/Books.csv"
BRONZE_PATH = "./data/lakehouse/bronze_books"
SILVER_PATH = "./data/lakehouse/silver_books"
GOLD_PATH   = "./data/lakehouse/gold_books"


def sep(label):
    print(f"\n{'='*60}\n  {label}\n{'='*60}")


# ---------------------------------------------------------------
# BRONZE: land the raw CSV exactly as-is. No cleaning, no rejects.
# This is the permanent, replayable source of truth.
# ---------------------------------------------------------------
def inject_bad_rows(df):
    """
    Deliberately append a few broken rows that a real upstream feed
    could realistically produce, so the Silver rejection path in
    clean_and_validate() actually has something to catch and quarantine.
    Bronze must still land these AS-IS -- rejection only happens later,
    at Silver -- so this is added here, before the Bronze write.
    """
    bad_rows = pd.DataFrame([
        {"Title": None, "Author": "Ghost Author", "Genre": "fiction",
         "Height": 200, "Publisher": "Unknown"},              # missing Title
        {"Title": "Negative Height Book", "Author": "Test Author",
         "Genre": "fiction", "Height": -15, "Publisher": "Test"},  # invalid Height
        {"Title": "Impossibly Tall Book", "Author": "Test Author",
         "Genre": "fiction", "Height": 9999, "Publisher": "Test"}, # invalid Height
    ])
    return pd.concat([df, bad_rows], ignore_index=True)


def load_bronze():
    sep("BRONZE - raw landing")
    df = pd.read_csv(RAW_CSV)
    df = inject_bad_rows(df)

    # A business key is required for MERGE later. This raw dataset has
    # no natural unique id, so we assign one now, at Bronze, based on
    # row position -- this becomes the permanent identity of each book.
    df.insert(0, "book_id", range(1, len(df) + 1))

    shutil.rmtree(BRONZE_PATH, ignore_errors=True)
    write_deltalake(BRONZE_PATH, df, mode="overwrite")

    print(f"Bronze written: {len(df)} rows, {df.shape[1]} columns (includes 3 injected bad rows)")
    print(df.tail(3))
    return df


# ---------------------------------------------------------------
# SILVER: governed, cleaned table. Quality rules applied here, not
# at Bronze -- so Bronze always stays a complete, unmodified replay
# source if a Silver rule ever needs to be recomputed.
# ---------------------------------------------------------------
def clean_and_validate(df):
    good = df.copy()

    # Completeness: Author and Publisher may be missing in this dataset;
    # Title must never be missing -- drop rows without one.
    good = good[good["Title"].notna() & (good["Title"].str.strip() != "")]

    # Validity: Height must be a sane positive number (a book height of
    # 0 or negative, or an absurd outlier, is not physically possible).
    good = good[(good["Height"] > 0) & (good["Height"] < 400)]

    # Consistency: normalize Genre to lowercase/stripped so "Fiction"
    # and "fiction" aren't silently treated as different categories.
    good["Genre"] = good["Genre"].str.strip().str.lower()

    # Fill missing Publisher with an explicit marker rather than a
    # silent blank, so Gold aggregations don't drop these rows.
    good["Publisher"] = good["Publisher"].fillna("Unknown")

    # Reset the index after filtering -- otherwise the leftover,
    # non-contiguous pandas index leaks into the Delta write as a
    # stray "__index_level_0__" column.
    good = good.reset_index(drop=True)

    rejected = df[~df["book_id"].isin(good["book_id"])]
    return good, rejected


def load_silver(bronze_df):
    sep("SILVER - governed, cleaned table")
    good, rejected = clean_and_validate(bronze_df)

    shutil.rmtree(SILVER_PATH, ignore_errors=True)
    write_deltalake(SILVER_PATH, good, mode="overwrite")

    print(f"Silver written: {len(good)} rows accepted, {len(rejected)} rejected")
    if len(rejected) > 0:
        print("Rejected rows:")
        print(rejected[["book_id", "Title", "Height"]])
    return good


# ---------------------------------------------------------------
# GOLD: a genuine aggregate, computed FROM Silver. Not a copy.
# ---------------------------------------------------------------
def load_gold(silver_df):
    sep("GOLD - genuine aggregate (avg book height + count per genre)")
    gold_df = (
        silver_df.groupby("Genre")
        .agg(
            book_count=("book_id", "count"),
            avg_height=("Height", "mean"),
        )
        .reset_index()
        .sort_values("book_count", ascending=False)
    )
    gold_df["avg_height"] = gold_df["avg_height"].round(1)

    shutil.rmtree(GOLD_PATH, ignore_errors=True)
    write_deltalake(GOLD_PATH, gold_df, mode="overwrite")

    print(f"Gold written: {len(gold_df)} genre-level aggregate rows")
    print(gold_df)
    return gold_df


# ---------------------------------------------------------------
# SCHEMA ENFORCEMENT: prove a mismatched write is actually rejected.
# ---------------------------------------------------------------
def demo_schema_enforcement():
    sep("SCHEMA ENFORCEMENT - attempt a mismatched write (expect rejection)")
    bad_df = pd.DataFrame([{
        "book_id": 9999,
        "Title": "Ghost Row",
        "Author": "Nobody",
        "Genre": "mystery",
        "Height": 200,
        "Publisher": "Unknown",
        "discount_price": 5.0,   # column that does not exist in Silver's schema
    }])
    try:
        write_deltalake(SILVER_PATH, bad_df, mode="append", schema_mode="merge" if False else None)
        print("Write unexpectedly SUCCEEDED -- this should not happen.")
    except DeltaError as e:
        print(f"Write REJECTED as expected: {type(e).__name__}")
        print(f"  {str(e)[:200]}")


# ---------------------------------------------------------------
# MERGE / UPSERT: a real Delta MERGE keyed on book_id.
# ---------------------------------------------------------------
def demo_merge():
    sep("MERGE / UPSERT - correct a Height value, insert one new book")
    dt = DeltaTable(SILVER_PATH)

    updates = pd.DataFrame([
        # existing book_id 1 -> price/height correction (UPDATE)
        {"book_id": 1, "Title": "Fundamentals of Wavelets", "Author": "Goswami, Jaideva",
         "Genre": "signal_processing", "Height": 230, "Publisher": "Wiley"},
        # new book_id not present in Silver -> INSERT
        {"book_id": 9000, "Title": "Designing Data-Intensive Applications", "Author": "Kleppmann, Martin",
         "Genre": "computer_science", "Height": 235, "Publisher": "O'Reilly"},
    ])

    (
        dt.merge(
            source=updates,
            predicate="target.book_id = source.book_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    print("MERGE complete.")
    result = dt.to_pandas()
    print(result[result["book_id"].isin([1, 9000])])


# ---------------------------------------------------------------
# TRANSACTION LOG + TIME TRAVEL: prove ACID history is real.
# ---------------------------------------------------------------
def demo_log_and_time_travel():
    sep("TRANSACTION LOG + TIME TRAVEL")
    dt = DeltaTable(SILVER_PATH)

    history = dt.history()
    print(f"Silver table has {len(history)} commits in its transaction log:")
    for entry in history:
        print(f"  version {entry.get('version')}: {entry.get('operation')}")

    print("\nReading Silver as of version 0 (before the MERGE):")
    dt_v0 = DeltaTable(SILVER_PATH, version=0)
    print(dt_v0.to_pandas()[["book_id", "Title", "Height"]].head(3))


def main():
    bronze_df = load_bronze()
    silver_df = load_silver(bronze_df)
    load_gold(silver_df)
    demo_schema_enforcement()
    demo_merge()
    demo_log_and_time_travel()
    print("\nBronze / Silver / Gold Delta Lakehouse complete.")


if __name__ == "__main__":
    main()
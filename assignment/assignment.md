# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: Join the `metadata_pl` and `ratings_pl` DataFrames in Polars, then calculate the average rating by `genre` and by `original_language` (separately).

Answer:

```python
import polars as pl

# If data !loaded into mem use `pl.scan_csv("movies_metadata.csv")` 
# instead of `pl.read_csv()` so the data never fully loads into RAM at all.

# 1. Lazy Query Plan.
joined_lazy = (
    metadata_pl.lazy()      # Convert to LazyFrame.
# Convert to Int64, take the "id" column which was text/strings
# and casts it to 64-bit integers to match the data tye in ratings_pl
# strict=false prevents a crash if !number, return null.
    .with_columns(pl.col("id").cast(pl.Int64, strict=False))
    .drop_nulls(subset=["id"])  # Drop nulls because you can't join on null ID
# Ensure unique "id" to prevent memory explosion as duplicate movie IDs
# would multiply against millions of rating rows which would result in an
# an explosion of many-to-many joins which would freeze the server.
    .unique(subset=["id"])
    .join(
        ratings_pl.lazy(),  # Convert to LazyFrame
        left_on="id",
        right_on="movieId",
        how="inner"         # Only keep rows where movie exists in both tables
    )
)

# ---------- Aggregate and Execute ----------
# 2. Calculate avg_rating by genre using joined_lazy from 1. 
avg_rating_by_genre = (
    joined_lazy
    .group_by("genre")
# Look at "rating" column, calculate mean, and rename new output column
# "avg_rating" so it looks clean.
    .agg(pl.col("rating").mean().alias("avg_rating"))
    .collect()    # Execute on CPU/GPU optimized plan.
)

# 3. Calculate avg_rating by original_language, use joined_lazy from 1.
avg_rating_by_language = (
    joined_lazy
    .group_by("original_language")
    .agg(pl.col("rating").mean().alias("avg_rating"))
# Again collect() pulls the trigger and executes this specific pipeline.
    .collect()
)

# 4. Result.
print(avg_rating_by_genre)
print(avg_rating_by_language)

```

### Question 2

Question: Calculate the average total amount for vendors with at least 5 trips from the NYC Taxi Trip Data.

Answer:

```python



```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.

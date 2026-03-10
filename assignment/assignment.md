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

# 1. Join the DataFrames
# Replace "id" with the actual common column name
joined_pl = metadata_pl.join(ratings_pl, on="id", how="inner)

# 2. Calculate avg_rating by genre
avg_rating_by_genre = (
  joined_pl
  .group_by("genre")
  .agg(pl.col("rating").mean().alias("avg_rating")
)

# 3. Calculate avg_rating by original_language
avg_rating_by_language = (
  joined_pl
  .group_by("original_language")
  .agg(pl.col("rating").mean().alias("avg_rating")
)

# 4. Display results
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

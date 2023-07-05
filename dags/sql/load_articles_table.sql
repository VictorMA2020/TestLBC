INSERT INTO dwh.articles(
    date,
    author,
    headline,
    type,
    category,
    rating,
    genre,
    body,
    url
)
SELECT
    date,
    author,
    headline,
    type,
    category,
    rating,
    genre,
    body,
    url
FROM
    dwh_tmp.articles_cleaned_{{ ds_nodash }}
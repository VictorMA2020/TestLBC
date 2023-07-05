SELECT
    date,
    INITCAP(TRIM(author)) AS author,
    TRIM(headline) AS headline,
    INITCAP(TRIM(type)) AS type,
    INITCAP(TRIM(category)) AS category,
    SAFE_CAST(TRIM(rating) AS INT64) AS rating,
    INITCAP(TRIM(genre)) AS genre,
    TRIM(body) AS body,
    TRIM(url) AS url
FROM
    dwh_tmp.articles_raw_{{ ds_nodash }}
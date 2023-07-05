CREATE TABLE IF NOT EXISTS dwh.articles (
    date DATE NOT NULL OPTIONS(description='Date of data extraction'),
    author STRING OPTIONS(description='Author of the article'),
    headline STRING OPTIONS(description='Title of the article'),
    type STRING OPTIONS(description='Type of article (Review, Analysis, Interview etc.)'),
    category STRING OPTIONS(description='Article category (Film, Culture, Sport etc.)'),
    rating INT64 OPTIONS(description='Rating of the subject reviewed (for type = Review only)'),
    genre STRING OPTIONS(description='Genre of film (for category = film only)'),
    body STRING OPTIONS(description='Full article text'),
    url STRING OPTIONS(description='Link to article on theguardian website')
)
OPTIONS(description="Table containing articles information coming from theguardian.com, partitioned by date")
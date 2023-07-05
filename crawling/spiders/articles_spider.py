from datetime import datetime

from scrapy import Request, Spider


class GuardianSpider(Spider):
    name = 'articles'
    data_file = 'articles.csv'
    start_urls = [
        'https://www.theguardian.com/international',
        # Not very proud of this link hardcoding here, but I didn't find a page on the website where these film genres were available
        'https://www.theguardian.com/film/actionandadventure',
        'https://www.theguardian.com/film/drama',
        'https://www.theguardian.com/film/crime',
        'https://www.theguardian.com/film/family',
        'https://www.theguardian.com/film/horror',
        'https://www.theguardian.com/film/documentary',
        'https://www.theguardian.com/film/romance',
        'https://www.theguardian.com/film/sciencefictionandfantasy',
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse, cb_kwargs={'start_url': url})

    def parse(self, response, start_url):
        articles = response.xpath('//*[@data-link-name="article"]/@href').getall()
        with open(self.data_file, 'w+') as f:
            f.write('date; author; headline; type; category; rating; genre; body; url\n')
        for article_link in articles: 
            # We don't want to crawl the live section of the website, as it is almost constantly changing its structure is different from other regular articles
            if article_link.split('/')[4] != 'live':
                yield response.follow(
                    article_link, 
                    callback=self.get_article_content, 
                    cb_kwargs={'article_link': article_link, 'start_url': start_url}
                )
        return("Parsing done")

    def get_article_content(self, response, article_link, start_url):
        # Date
        date_raw = response.xpath('//*[@class="dcr-eb59kw"]//text()').get()
        try: date = datetime.strptime(date_raw, '%a %d %b %Y %H.%M BST').date() if date_raw else datetime.today().date()
        except ValueError: date = datetime.strptime(date_raw, '%a %d %b %Y %H.%M GMT').date() if date_raw else datetime.today().date()
        # Author
        author = response.xpath('//*[@aria-label="Contributor info"]//a[@rel="author"]/text()').get() or "N/A"
        author = author.strip()

        # Headline
        headline = response.xpath('//*[@data-gu-name="headline"]//text()').getall() or ["N/A"]

        # Type
        type = response.xpath('//*[@data-gu-name="headline"]//a[@class="dcr-1csdh30"]//text()').get() or "N/A"
        type = type.strip()

        # Category
        category = response.xpath('//*[@data-link-name="article section"]//text()').get() or "N/A"
        category = category.strip()

        # Rating (for reviews only)
        if type == "Review":
            rating_raw = response.xpath('//*[@class="dcr-1tlk9ix"]//path[@fill="#121212"]').getall()
            rating = len(rating_raw)
        else: rating = "N/A"

        # Genre (for movies only)
        genre = start_url.split("/")[4] if start_url.split("/")[3] == "film" else "N/A"

        # Body
        body = response.xpath('//*[@data-gu-name="body"]//text()').getall()
        for i in range(len(body)):
            body[i] = body[i].strip().replace(';', ',')  # Necessary to avoid csv conflicts

        # Write local file with crawled data
        with open(self.data_file, 'a') as f:
            f.write(f'{date}; {author}; {headline}; {type}; {category}; {rating}; {genre}; {body}; {article_link}\n')
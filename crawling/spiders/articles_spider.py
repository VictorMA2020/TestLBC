from datetime import datetime

from scrapy import Request, Spider


class GuardianSpider(Spider):
    name = 'articles'
    data_file = 'articles.csv'
    start_urls = ['https://www.theguardian.com/international']

    def parse(self, response):
        articles = response.xpath('//*[@data-link-name="article"]/@href').getall()
        with open(self.data_file, 'w+') as f:
            f.write('date; author; headline; body\n')
        for article_link in articles: 
            # We don't want to crawl the live section of the website, as it is almost constantly changing its structure is different from other regular articles
            if article_link.split('/')[4] != 'live':
                yield response.follow(article_link, callback=self.get_article_content)
        return('Parsing done')

    def get_article_content(self, response):
        headline = response.xpath('//*[@data-gu-name="headline"]//text()').get()
        body = response.xpath('//*[@data-gu-name="body"]//text()').getall()
        for body_part in body:
            body_part = body_part.replace(';', ',')  # Necessary to avoid csv conflicts
        author = response.xpath('//*[@aria-label="Contributor info"]//a[@rel="author"]/text()').get()
        date_raw = datetime.strptime(response.xpath('//*[@class="dcr-eb59kw"]//text()').get(), '%a %d %b %Y %H.%M BST')
        date = datetime.strftime(date_raw, '%Y-%m-%d')
        with open(self.data_file, 'a') as f:
            f.write(f'{date}; {author}; {headline}; {body}\n')
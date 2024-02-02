import scrapy
import string
import re


class MoviesSpider(scrapy.Spider):
    name = "movies"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ["https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        for movie in response.xpath('//div[@id="mw-pages"]//li'):
            link = movie.xpath('.//a/@href').get()
            if link:
                full_url = response.urljoin(link)
                yield response.follow(full_url, callback=self.parse_movie)

        next_page = response.xpath('//a[contains(@href, "pagefrom=") and contains(text(), "Следующая страница")]/@href').get()
        if next_page is not None:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, self.parse_page)

    def parse_movie(self, response):
        title = response.xpath('//*[@id="firstHeading"]/span/text()').get()
        if not title:
            title = response.xpath('//*[@id="firstHeading"]/i/text()').get()

        genres = response.xpath('//a[contains(text(), "Жанр") or contains(text(), "Жанры")]/../following-sibling::td//text()[not(ancestor::sup)]').getall()
        genres = [genre.strip() for genre in genres if genre.strip() and '[' not in genre and ']' not in genre and genre.strip(string.punctuation).strip()]

        directors = response.xpath('//th[contains(text(), "Режиссёр") or contains(text(), "Режиссёры")]/following-sibling::td//text()[not(ancestor::sup)]').getall()
        directors = [director.strip() for director in directors if director.strip() and not any(char in director for char in "[]()")]

        countries = response.xpath('//th[contains(text(), "Страна") or contains(text(), "Страны")]/following-sibling::td//text()[not(ancestor::sup)]').getall()
        countries = [country.strip() for country in countries if country.strip() and '[' not in country and ']' not in country]

        dates = response.xpath('//th[contains(text(), "Год") or contains(text(), "Первый показ")]/following-sibling::td//text()[not(ancestor::sup)]').getall()
        year_pattern = re.compile(r'\b\d{4}(?:-\d{4})?\b')
        year = None
        for date in dates:
            match = year_pattern.search(date)
            if match:
                year = match.group()
                break

        yield {
            'title': title,
            'genres': genres,
            'directors': directors,
            'countries': countries,
            'year': year
        }




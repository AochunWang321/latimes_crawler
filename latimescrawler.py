import scrapy
from scrapy import Selector
import csv
from scrapy.exceptions import CloseSpider


class LatimesSpider(scrapy.Spider):
    name = "latimes"
    allowed_domains = ["latimes.com"]
    start_urls = ["https://www.latimes.com"]

    def __init__(self, *args, **kwargs):
        super(LatimesSpider, self).__init__(*args, **kwargs)
        # 写fetch_NewsSite.csv
        self.fetch_csvfile = open("fetch_NewsSite.csv", 'w', newline='', encoding='utf-8')
        self.fetch_writer = csv.writer(self.fetch_csvfile)
        self.fetch_writer.writerow(['url', 'status'])
        # 写visit_NewsSite.csv
        self.visit_csvfile = open("visit_NewsSite.csv", 'w', newline='', encoding='utf-8')
        self.visit_writer = csv.writer(self.visit_csvfile)
        self.visit_writer.writerow(['url', 'size_bytes', 'external_links_count', 'content_type'])
        # 写 urls_NewsSite.csv
        self.urls_csvfile = open("urls_NewsSite.csv", 'w', newline='', encoding='utf-8')
        self.urls_writer = csv.writer(self.urls_csvfile)
        self.urls_writer.writerow(['url', 'type'])
        # 判断去重列表
        self.visited_urls = set()
        # 统计次数的不要改动
        self.fetch_count = 0
        # 最大计数
        self.max_count = 20000

    def __del__(self):
        # 爬虫停止 关闭创建的csv对象
        self.fetch_csvfile.close()
        self.visit_csvfile.close()
        self.urls_csvfile.close()

    def parse(self, response):
        if self.fetch_count > self.max_count:
            raise CloseSpider(f'已经抓取了{self.max_count}条数据.')
        # 更新URL计数器
        # 写 fetch_NewsSite.csv
        self.fetch_writer.writerow([response.url, response.status])
        self.fetch_count += 1
        print(f"当前 {self.fetch_count} 个")
        sel = Selector(text=response.text)
        # link_items = sel.css('a.link.promo-placeholder::attr(href)').re(r'https?://[^"]+')
        link_items = sel.css('a::attr(href)').re(r'https?://[^"\'\s]+')
        # 判断Content-Type 写 visit_NewsSite.csv
        mime_type = response.headers.get('Content-Type').decode('utf-8').split(';')[0]
        if 'text/html' in mime_type:
            content_type = 'html'
        elif 'image/' in mime_type:
            content_type = 'img'
        elif 'application/pdf' in mime_type:
            content_type = 'pdf'
        elif 'application/msword' in mime_type or 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in mime_type:
            content_type = 'doc'
        else:
            content_type = 'other'
        self.visit_writer.writerow([response.url, len(response.text.encode('utf-8')), len(link_items), content_type])

        for link in link_items:
            # 检查列表是否已经解析过
            if link not in self.visited_urls:
                # 如果没解析过增加到列表中
                self.visited_urls.add(link)  # 将URL添加到已访问集合中
                # 写入 urls_NewsSite.csv
                url_type = 'a' if "www.latimes.com" in response.url else 'b'
                self.urls_writer.writerow([response.url, url_type])
                yield scrapy.Request(link, callback=self.parse, errback=self.handle_error)
            else:
                # 写入 urls_NewsSite.csv
                url_type = 'a' if "www.latimes.com" in response.url else 'b'
                self.urls_writer.writerow([response.url, url_type])


    def handle_error(self, failure):
        # 获取失败的URL
        url = failure.request.url
        # 记录失败的状态码；如果没有状态码，记录为0
        status = failure.value.response.status if failure.value.response else 0
        # 写入 fetch_NewsSite.csv
        self.fetch_writer.writerow([url, status])
        self.fetch_count += 1
        print(f"当前 {self.fetch_count} 个")

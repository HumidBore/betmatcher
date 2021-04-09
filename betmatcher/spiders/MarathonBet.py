# -*- coding: utf-8 -*-
import scrapy
from betmatcher.spiders.templateSpider import TemplateSpider
import json
from datetime import datetime, timedelta
import re


class MarathonbetSpider(scrapy.Spider, TemplateSpider):
    name = 'MarathonBet'
    allowed_domains = ['marathonbet.it']

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        # 'DOWNLOAD_DELAY': 0.27, 
        # 'RANDOMIZE_DOWNLOAD_DELAY': False,
        # 'RETRY_HTTP_CODES': [429],
        # 'RETRY_TIMES': 3,
        # 'DOWNLOADER_MIDDLEWARES': {'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        #                             'betmatcher.middlewares.UserAgentRotatorMiddleware': 543,
        #                             'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
        #                             'betmatcher.middlewares.TooManyRequestsRetryMiddleware': 544,
        #                         },
        'ITEM_PIPELINES': {'betmatcher.pipelines.BetmatcherPipeline': 300
                            },
        # 'LOG_LEVEL': 'INFO',  #to remove output (N.B. OUTPUT SLOWS DOWN THE PROGRAMME A LOT)
        # 'FEED_EXPORT_ENCODING': 'UTF-8'  #specifies the encoding format
    }

    splitterMarathonBet = ' vs '

    MONTHS = {'Gen': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Mag': '05', 'Giu': '06',
             'Lug': '07', 'Ago': '08', 'Set': '09', 'Ott': '10', 'Nov': '11', 'Dic': '12'}

    def start_requests(self):
        yield scrapy.Request(url='https://www.marathonbet.it/it/betting/11?cpcids=all&interval=ALL_TIME', 
                            callback=self.parse)
        

    def parse(self, response):
        base_url = 'https://www.marathonbet.it'

        links = response.xpath('//div[@class="hidden-links"]//@href').extract()
        links = [link for link in links if ("Football" in link) and (link.count('/') >= 4) and (not 'Outright' in link)]     #a link with <= 4 / is a link to "Italy", "England" etc., not to a tournament. Outright shows results

        for link in links:
            if "Italy/Serie+A" in link:
                yield scrapy.Request(url=base_url+link, callback=self.parseTournament)

    def parseTournament(self, response):
        for event in response.xpath('//div[@class="bg coupon-row"]'):
            squadra1, squadra2 = event.xpath('./@data-event-name').get().split(self.splitterMarathonBet, 1)
            squadra1 = re.sub(r'\b\w{1,2}\b', '', re.sub(r'[.\(\)\[\]?!]', '', squadra1))
            squadra2 = re.sub(r'\b\w{1,2}\b', '', re.sub(r'[.\(\)\[\]?!]', '', squadra2))
            eventName = re.sub(r' +', ' ', f'{squadra1} - {squadra2}'.strip()).upper()  
            date = event.xpath("//td[contains(@class,'date')]").get()
            if len(date) == 17: # 27 Apr 2021 12:00
                d, month, y, hh_mm = date.split(' ')
                m = monthes.index(month.capitalize()) + 1
                hh, mm  = hh_mm.split(':')
                date = dt.replace(day = int(d), month = m, year = int(y), hour = int(hh), minute = int(mm), second = 0).isoformat(' ', timespec='seconds')
            elif len(date) == 12: # 28 Apr 10:00
                d, month, hh_mm = date.split(' ')
                m = monthes.index(month.capitalize()) + 1
                hh, mm  = hh_mm.split(':')
                date = dt.replace(day = int(d), month = m, hour = int(hh), minute = int(mm), second = 0).isoformat(' ', timespec='seconds')
            elif len(date) == 5: # 23:00
                hh, mm = date.split(':')
            
            for outcome in event.xpath("./table/tbody/tr/td[contains(@class, 'height-column-with-price')]/span"):
                yield {
                    # 'tournament': response.request.meta['tournamentName'],
                    'betRadarID': -1,
                    # 'event': eventBody.get('name'),
                    'event': eventName,
                    # 'date': dateUTC,
                    'bet': outcome.xpath("./@data-selection-key").get(),
                    'betOdd': outcome.xpath("./text()").get()
                }


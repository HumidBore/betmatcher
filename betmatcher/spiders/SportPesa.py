# -*- coding: utf-8 -*-
import scrapy
from betmatcher.spiders.templateSpider import TemplateSpider
import json
import re
from datetime import datetime

class SportpesaSpider(scrapy.Spider, TemplateSpider):
    name = 'SportPesa'
    allowed_domains = ['www.sportpesa.it']

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.2,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'RETRY_HTTP_CODES': [429],
        'RETRY_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                                    'betmatcher.middlewares.UserAgentRotatorMiddleware': 543,
                                    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
                                    'betmatcher.middlewares.TooManyRequestsRetryMiddleware': 544,
                                },
        'ITEM_PIPELINES': {'betmatcher.pipelines.SQLlitePipeline': 300
                            },
        'HTTPCACHE_ENABLED': False,
        'LOG_LEVEL': 'INFO',  #to remove output (N.B. OUTPUT SLOWS DOWN THE PROGRAMME A LOT)
        'FEED_EXPORT_ENCODING': 'UTF-8'  #specifies the encoding format
    }

    #used to split an event's name and get participants (e.g. "Juventus-Napoli" gives "Juventus"  and "napoli")
    splitterSportPesa = '-'

    def start_requests(self):   #sends a request to get the schedule (tree) (it contains all the info to find urls)
        yield scrapy.Request(url='https://www.sportpesa.it/SportsBookAPI.svc/PrematchSchedule', 
                            callback = self.parse)

  
    #parses the tournament tree, sending a request to each football section (country) and for each tournament (serie a, serie b ...) 
    def parse(self, response):
        jsonResponseSports = json.loads(response.body)    #to get Sports and competition codes (json.loads parses body string as a json object)

        for country in jsonResponseSports[0].get('Children'):    #football is code 0, Children are countries (Italy, Spain...)
            self.now = datetime.now()
            for tournament in country.get('Children'):  #each child is a tornament (serie A, serie b...)
                tournament_id = tournament.get('Id')        #gets the id to send the request

                # if tournament_id == 195626:
                yield scrapy.Request(
                        url = f'https://www.sportpesa.it/SportsBookAPI.svc/Odds/{tournament_id}',
                        callback=self.parseTournament,
                        meta = {'tournamentName': tournament.get('Name')}
                    )
    
    def parseTournament(self, response):
        jsonResponseTournament = json.loads(response.body)
        
        matches = {}

        for event in jsonResponseTournament.get("events"):
            dateUTC = datetime.fromtimestamp(float(re.sub(r'^.*?\(', '', re.sub(r'\+.*?$', '', event.get('StartDate')))) / 1000.0)  #regular expression: removes all characters before '(' and after '+'
            if dateUTC > self.now:
                matches[event.get('Id')] = [event.get('BetRadarId'), #index 0 is the betRadarID, index 1 is the name, index 2 is the date
                                        event.get('Name'), 
                                        dateUTC]
            else:
                print(self.name, "evento live, passo al successivo")
                
            
        
        for bet in jsonResponseTournament.get("eventsOutcome"):
            try:

                eventName = self.findEventName(matches.get(bet.get('EventId'))[1], self.splitterSportPesa)

                yield {
                'tournament': response.request.meta['tournamentName'],
                'betRadarID': matches[bet.get('EventId')][0],   #index 0 is the betRadarID
                # 'event': matches[bet.get('EventId')][1].replace('-', ' - ', 1 ),    #index 1 is the name
                # 'event': f'{squadra1} - {squadra2}',    #index 1 is the name
                'event': eventName,    #index 1 is the name
                'date':  matches[bet.get('EventId')][2],     #index 2 is the date
                'bet': bet.get('Kind').get('Name') + self.BET_SEPARATOR + bet.get('Name'),
                'betOdd': bet.get('Odds')
                }
            except ValueError:
                pass
                # print('Quota non di un match:', match)
            
            
          
# -*- coding: utf-8 -*-
import scrapy
from betmatcher.spiders.templateSpider import TemplateSpider
from datetime import datetime, timedelta
import json
import re

class A888sportSpider(scrapy.Spider, TemplateSpider):
    name = '888Sport'
    allowed_domains = ['www.888sport.it', 'eu-offering.kambicdn.org']


    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.25,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'RETRY_HTTP_CODES': [429],
        'RETRY_TIMES': 3,
        'DOWNLOADER_MIDDLEWARES': {'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                                    'betmatcher.middlewares.UserAgentRotatorMiddleware': 543,
                                    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
                                    'betmatcher.middlewares.TooManyRequestsRetryMiddleware': 544,
                                },
        'ITEM_PIPELINES': {'betmatcher.pipelines.AzureCloudDatabasePipeline': 300
                            },
        'LOG_LEVEL': 'INFO',  #to remove output (N.B. OUTPUT SLOWS DOWN THE PROGRAMME A LOT)
        'FEED_EXPORT_ENCODING': 'UTF-8'  #specifies the encoding format
    }

    MINIMUM_NUMBER_OF_BETS = 10  #If an event doesn't have enough bets (>), must be discarded because it's not useful

    #If a bet type contains one of the elements of the following list, it won't be added in the output
    EXCLUDED_BET_TYPES = ['MARCATORE', 'HANDICAP', 'CARTELLINO', 'CORNER 1', 'HAND CORNER', 'TEMPO X + U/O TEMPO Y', 'MINUTO', 'INTERVALLO' ]

    #used to split an event's name and get participants (e.g. "Juventus - Napoli" gives "Juventus"  and "napoli")
    splitter888Sport = ' - '


    def start_requests(self):   #sends a request to get all the groups (countries and tournaments)
        yield scrapy.Request(url='https://eu-offering.kambicdn.org/offering/v2018/888it/group.json?lang=it_IT&market=IT&client_id=2&channel_id=1&ncid=1614704340494', 
                            callback = self.parse)

  
    #parses group json file, sending a request to each football section (country) and for each tournament (serie a, serie b ...) 
    def parse(self, response):
        jsonResponseSports = json.loads(response.body)    #to get Sports and competition codes (json.loads parses body string as a json object)
        for country in jsonResponseSports.get("group").get('groups')[0].get('groups'):    #football is index 0, categories are countries (Italy, Spain...)
            countryKey = country.get("termKey") #to get the country name
            self.now = datetime.now()
            if country.get("groups"):
                for tournament in country.get("groups"):
                    if tournament.get("eventCount") != None and tournament.get("eventCount") > self.MINIMUM_NUMBER_OF_BETS:
                        tournamentKey = tournament.get('termKey')     #termKey is the name of the file containing the bets
                        yield scrapy.Request(
                            url = f'https://eu-offering.kambicdn.org/offering/v2018/888it/listView/football/{countryKey}/{tournamentKey}.json?lang=it_IT&market=IT&client_id=2&channel_id=1&ncid=1614704340502&useCombined=true',
                            callback=self.parseTournament
                        )
            else:
                if country.get("eventCount") != None and country.get("eventCount") > self.MINIMUM_NUMBER_OF_BETS:
                    yield scrapy.Request(
                            url = f'https://eu-offering.kambicdn.org/offering/v2018/888it/listView/football/{countryKey}.json?lang=it_IT&market=IT&client_id=2&channel_id=1&ncid=1614704340502&useCombined=true',
                            callback=self.parseTournament
                        )
            

    #parses the torurnament, sending a request to have the json object with all the event quotes
    def parseTournament(self, response):
        jsonResponseTournament = json.loads(response.body)    #response.body is a json file, got by the site's backend 
        for event in jsonResponseTournament.get("events"):    #each event has an id used to send another request to the event url
            yield scrapy.Request(
                url = f'https://eu-offering.kambicdn.org/offering/v2018/888it/betoffer/event/{event.get("event").get("id")}.json?lang=it_IT&market=IT&client_id=2&channel_id=1&ncid=1614707330403&includeParticipants=true',
                callback=self.parseEventBets
            )
            
            
                     
    #parses an event, extracting odds from the json object got as response
    def parseEventBets(self, response):
        jsonResponseEvent = json.loads(response.body)    #response.body is a json file, got by the site's backend 
        event = jsonResponseEvent.get('events')[0]

        tournament = event.get("group")
        
        #date using italian timezone
        date = datetime.fromisoformat(event.get("start").replace('Z', '')) + timedelta(hours=2) #need to add 2 hours to have italian time (UTC +2)
        if date < self.now:
            print(self.name, "evento live, passo al successivo")
            return
        try:
            eventName = self.findEventName(event.get('name'), self.splitter888Sport)

            for bet in jsonResponseEvent.get('betOffers'):    #bet is a type of bet, whose label is caught and stored right away
                betType = bet.get('criterion').get('label')     #getting the type, as a string (e.g. "esito finale 1x2")
                originalBetType = betType
                for outcome in bet.get("outcomes"):
                    if outcome.get('line'):
                        betType = f'{originalBetType} {round(float(outcome.get("line")) / 1000, 1)}'  #line is for example 2500 (stand for 2.5) in Under/Over
                    yield {
                        'tournament': tournament,
                        # 'betRadarID': None,
                        'betRadarID': -1,
                        'event': eventName,
                        'date': date,
                        'bet': betType + self.BET_SEPARATOR + outcome.get('label'),
                        'betOdd': outcome.get('oddsFractional')
                    }  
        except ValueError:
            pass
            # print('Quota non di un match:', match)


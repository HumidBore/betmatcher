# -*- coding: utf-8 -*-
import scrapy
from betmatcher.spiders.templateSpider import TemplateSpider
import json
import re
from datetime import datetime, timedelta
from decimal import Decimal


class SisalSpider(scrapy.Spider, TemplateSpider):
    name = 'Sisal'
    allowed_domains = ['www.sisal.it']

    custom_settings = {
        'CONCURRENT_REQUESTS': 9999999999,
        'DOWNLOAD_DELAY': 15,
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
        'HTTPCACHE_ENABLED': False,
        'LOG_LEVEL': 'INFO',  #to remove output (N.B. OUTPUT SLOWS DOWN THE PROGRAMME A LOT)
        'FEED_EXPORT_ENCODING': 'UTF-8'  #specifies the encoding format
    }

    #used to split an event's name and get participants (e.g. "Juventus-Napoli" gives "Juventus"  and "napoli")
    splitterSisal = ' - '

    
    def start_requests(self):   #sends a request to get the schedule (tree) (it contains all the info to find urls)
        yield scrapy.Request(url='https://www.sisal.it/api-betting/lettura-palinsesto-sport/palinsesto/prematch/alberaturaPrematch', 
                            callback = self.parse)

  
    #parses the tournament tree, sending a request to each football section (country) and for each tournament (serie a, serie b ...) 
    def parse(self, response):
        jsonResponseSports = json.loads(response.body)    #to get Sports and competition codes (json.loads parses body string as a json object)
       
        self.now = datetime.now()
        secondary_tournaments = []
        for tournament_id in jsonResponseSports.get('manifestazioneListByDisciplinaTutti').get('1'):    #football is code 1, and manifestazione is a tournament
            # self.now = datetime.now()   #to compare with event date and time to see if the event is live or not
            if jsonResponseSports.get('manifestazioneMap').get(tournament_id).get("topManifestazione"):
                tournament_name = jsonResponseSports.get('manifestazioneMap').get(tournament_id).get("descrizione")
                
                yield scrapy.Request(
                        url = f'https://www.sisal.it/api-betting/lettura-palinsesto-sport/palinsesto/prematch/schedaManifestazione/0/{tournament_id}',
                        callback=self.parseTournament,
                        meta = {'tournamentName': tournament_name, 'download_timeout': 40, 'max_retry_times': 2}
                    )
            elif (jsonResponseSports.get('eventsNumberByManifestazioneTutti').get(tournament_id) > 1):  #requests are precious, they must not be sent for tournament with only one event (which is probably an antepost)
                secondary_tournaments.append(tournament_id)
        
        for tournament_id in secondary_tournaments:    #football is code 1, and manifestazione is a tournament
            
            tournament_name = jsonResponseSports.get('manifestazioneMap').get(tournament_id).get("descrizione")
            
            yield scrapy.Request(
                    url = f'https://www.sisal.it/api-betting/lettura-palinsesto-sport/palinsesto/prematch/schedaManifestazione/0/{tournament_id}',
                    callback=self.parseTournament,
                    meta = {'tournamentName': tournament_name, 'download_timeout': 40, 'max_retry_times': 2}
                ) 
    
    def parseTournament(self, response):
        jsonResponseTournament = json.loads(response.body)
        
        matches = {}

        for event in jsonResponseTournament.get("avvenimentoFeList"):
            date = datetime.fromisoformat(event.get('data').replace('Z', '')) + timedelta(hours=2)
            if date > self.now:
                try:
                    betRadarID = event.get('externalProviderInfoList')[0].get('idAvvProviderLive')
                    if betRadarID: #if is not null
                        matches[event.get("codiceAvvenimento")] = {
                            'betRadarID': betRadarID,
                            'eventName': self.findEventName(event.get("descrizione"), self.splitterSisal),
                            'date': date
                        }
                except (ValueError,TypeError):  #in case of an odd not of a match (probably antepost)
                    pass
            else:
                print(self.name, "evento live, passo al successivo")
        
        
        for bet in jsonResponseTournament.get("infoAggiuntivaMap").values():
            codiceAvvenimento = bet.get("codiceAvvenimento")
            try:
                for outcome in bet.get("esitoList"):
                    yield {
                    'tournament': response.request.meta['tournamentName'],
                    'betRadarID': matches.get(codiceAvvenimento).get('betRadarID'),   
                    'event': matches.get(codiceAvvenimento).get('eventName'),   
                    'date':  matches.get(codiceAvvenimento).get('date'),     
                    'bet': bet.get('descrizione') + self.BET_SEPARATOR + outcome.get('descrizione'),
                    'betOdd': str(round(Decimal(outcome.get('quota')) / 100, 2))    #odd in integer format. In the database it is stored as varchar
                    }
            except AttributeError:  #may be raised when the event does not have betRadarID, so in the dict it has None, which raise this exception because does not have get method
                pass
        
            
            
          

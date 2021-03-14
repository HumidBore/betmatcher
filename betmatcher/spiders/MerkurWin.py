# -*- coding: utf-8 -*-
import scrapy
from betmatcher.spiders.templateSpider import TemplateSpider
import json
from datetime import datetime, timedelta
import re
#IT IS THE SAME AS AdmiralYes
class MerkurwinSpider(scrapy.Spider, TemplateSpider):

    name = 'MerkurWin'
    allowed_domains = ['merkur-win.it', 'scommesse2.merkur-win.it/']
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.27, 
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
        'LOG_LEVEL': 'INFO',  #to remove output (N.B. OUTPUT SLOWS DOWN THE PROGRAMME A LOT)
        'FEED_EXPORT_ENCODING': 'UTF-8'  #specifies the encoding format
    }

    MINIMUM_NUMBER_OF_BETS = 1  #If an event doesn't have enough bets (>), must be discarded because it's not useful

    #If a bet type contains one of the elements of the following list, it won't be added in the output
    EXCLUDED_BET_TYPES = ['MARCATORE', 'HANDICAP', 'CARTELLINO', 'CORNER 1', 'HAND CORNER', 'TEMPO X + U/O TEMPO Y', 'MINUTO', 'INTERVALLO' ]
    
    #used to split an event's name and get participants (e.g. "Juventus - Napoli" gives "Juventus"  and "napoli")
    splitterMerkurWin = ' - '


    def start_requests(self):   #sends a request to get the tree (it contains all the info to find urls)
        yield scrapy.Request(url='https://scommesse2.merkur-win.it/backend/api/SportInfos/TournamentsTree', 
                            callback = self.parse)

  
    #parses the tournament tree, sending a request to each football section (country) and for each tournament (serie a, serie b ...) 
    def parse(self, response):
        jsonResponseSports = json.loads(response.body)    #to get Sports and competition codes (json.loads parses body string as a json object)
        for country in jsonResponseSports.get("sports").get('1').get("categories").values():    #football is code 1, categories are countries (Italy, Spain...)
            self.now = datetime.now()
            for tournament in country.get('tournaments').values():
                
                sportCode = tournament.get('sportCode')     #sportCode (football is '1')
                tournamentCode = tournament.get('id')       #tournamentCode define the competition (serie a, premier league...)
                tournamentName = tournament.get('name')
                betCode = tournament.get('betCode')     #the same for all categories 
                betSection = tournament.get('betSection')       #the same for all categories 
                
                if sum(tournament.get('eventsCounter').values()) > self.MINIMUM_NUMBER_OF_BETS: #finds the number of events in the tournament 
                    yield scrapy.Request(
                        url = f'https://scommesse2.merkur-win.it/backend/api/SportInfos/TournamentEvents?sportCode={sportCode}&tournamentCode={tournamentCode}&betCode={betCode}&betSection={betSection}&tag=',
                        callback=self.parseTournament,
                        meta={'tournamentName': tournamentName}
                    )
                else:
                    pass
                    # print('Tournament: ', tournamentName, 'HAS NOT ENOUGH BETS')
            


    #parses the torurnament, sending a request to have the json object with all the event quotes
    def parseTournament(self, response):
        jsonResponseMatchday = json.loads(response.body)    #response.body is a json file, got by the site's backend 
        for eventKey in jsonResponseMatchday.get("body").keys():    #each event has a code in the format scheduleCode.eventCode
            scheduleCode , eventCode = eventKey.split('.', 1)  #splits the codes (maxsplit = 1 to make sure only two string are generated)
            
            yield scrapy.Request(
                url = f'https://scommesse2.merkur-win.it/backend/api/SportInfos/EventDetails?scheduleCode={scheduleCode}&eventCode={eventCode}',
                callback=self.parseEventBets,
                meta={'scheduleCode': scheduleCode, 'eventCode': eventCode, 'tournamentName': response.request.meta['tournamentName']}
            )
            
                     

    #parses an event, extracting odds from the json object got as response
    def parseEventBets(self, response):
        scheduleCode = response.request.meta['scheduleCode']
        eventCode = response.request.meta['eventCode']
        
        jsonResponseEvent = json.loads(response.body)    #response.body is a json file, got by the site's backend 
        
        #eventBody contains all the values of the bets
        eventBody = jsonResponseEvent.get("body").get(f'{scheduleCode}.{eventCode}')
        
        #eventHead contains all the types of bet, corresponding to the values in eventBody
        eventHead = jsonResponseEvent.get("head")

        dateUTC = datetime.fromisoformat(eventBody.get('dateTimeUTC').replace('Z', '')) + timedelta(hours=1)
        if dateUTC < self.now:
            print(self.name, "evento live, passo al successivo")
            return
        try:
            eventName = self.findEventName(eventBody.get('name'), self.splitterMerkurWin)

            betRadarID = eventBody.get('externalReferences').get('BetRadar').get('matchId')
        
            for betCode in eventBody.get('bets').keys():    #betCode is the code of betType (example: 1X2 has code 3)
                betType = eventHead.get(betCode).get('name')     #getting the type, as a string, known the code
                
                #checks if the bet is excluded (type 'MARCATORE' excluded to lighten the software)
                if not self.isExcludedType(betType):
                                                                                    #checks status (0 means the bet is not active)
                    if (not eventBody.get('bets').get(betCode).get('hasSpread')) and  eventBody.get('bets').get(betCode).get('additionalInfos').get('0').get('status') == 1:
                        for outcome in eventBody.get('bets').get(betCode).get('additionalInfos').get('0').get('outcome').values():
                            if outcome.get('status') == 1:
                                

                                yield {
                                    'tournament': response.request.meta['tournamentName'],
                                    'betRadarID': betRadarID,
                                    # 'event': eventBody.get('name'),
                                    'event': eventName,
                                    'date': dateUTC,
                                    'bet': betType + self.BET_SEPARATOR + outcome.get('label'),
                                    'betOdd': outcome.get('val')
                                }

                    else:
                        for spreadOption in eventHead.get(betCode).get('spreadOptions').values():
                            spreadOptionKey = spreadOption.get('key')
                            spread = spreadOption.get('display')
                            if eventBody.get('bets').get(betCode).get('additionalInfos').get(spreadOptionKey).get('status') == 1:   #checks whether the bet is active or not
                                for outcome in eventBody.get('bets').get(betCode).get('additionalInfos').get(spreadOptionKey).get('outcome').values():
                                    if outcome.get('status') == 1:
                                        
                                        yield {
                                            'tournament': response.request.meta['tournamentName'],
                                            'betRadarID': betRadarID,
                                            # 'event': eventBody.get('name'),
                                            'event': eventName,
                                            'date': dateUTC,
                                            'bet': f'{betType} {spread}' + self.BET_SEPARATOR + outcome.get('label'),
                                            'betOdd': outcome.get('val')
                                        }
        except (AttributeError,ValueError):
            pass
            # print("AtributeError", eventBody.get('name'), dateUTC)
        # except ValueError:
            # print('Quota non di un match:', match)
            

    #some types of bets are excluded, because not useful
    def isExcludedType(self, betType):
        for excludedType in self.EXCLUDED_BET_TYPES:
            if excludedType in betType:
                return True
        return False
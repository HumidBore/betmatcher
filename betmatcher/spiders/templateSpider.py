import re


class TemplateSpider (object):

    BET_SEPARATOR = ' <> '  #used to separate betType and bet (e.g. 1X2 <> 1)


    #function to parse a tournament. It extracts all the events link 
    # (e.g. 'Serie A' contains 10 events for each matchday. This function will send a request to each one, on order to get all the bets)
    def parseTournament(self, response):
        pass

    #function to parse an event. It extracts all the bets from a response containing regarding an event.
    # In some cases it is not necessary (e.g. SportPesa spider) because all the bets are inside the tournament json 
    # and because of that, extracted inside parseTournament function
    def parseEventBets(self, response):
        pass
    
    #returns a formatted eventName, removing some characters such as 'AC' , 'FC' which can made matching not possible
    #N.B. MAY RAISE ValueError, which must be handled inside the spider
    def findEventName(self, event, splitter):
        squadra1, squadra2 = re.sub(r'\b\w{1,2}\b', '', re.sub(r'[.\(\)\[\]?!]', '', event)).split(splitter, 1)
        return re.sub(r' +', ' ', f'{squadra1} - {squadra2}'.strip()).upper()  

import csv

from decimal import Decimal, InvalidOperation
class BetTypeAdjuster(object):
    # SPREADS_TO_APPEND = ['MULTIGOL', 'DOPPIA CHANCE IN + U/O', 'DOPPIA CHANCE OUT + U/O', 'DOPPIA CHANCE IN/OUT + U/O']
    # SPREADS_TO_REPLACE = ['TEMPO X - DOPPIA CHANCE IN', 'TEMPO X - DOPPIA CHANCE OUT', 'TEMPO X - DOPPIA CHANCE IN/OUT', 
    #                     'TEMPO X - U/O 0.5', 'TEMPO X - U/O 1.5', 'TEMPO X - U/O 2.5', 'SOMMA GOAL TEMPO X', 
    #                     'DC IN + GG/NG TEMPO X', 'DC OUT + GG/NG TEMPO X', 'DC IN/OUT + GG/NG TEMPO X' 
    #                     ]
    allowed_bets_filename = 'AllowedBets'

    def __init__(self, bookmaker):
        self.load_bets_to_replace(bookmaker)
        self.load_allowed_bets()

    def load_bets_to_replace(self, bookmaker):  #bookmaker will be spider.name used in open_spider pipeline
        self.STRING_TO_REPLACE = {}  #each bookmaker has a dict

        with open(f'.\\Resources\\{bookmaker}.csv') as csv_file:   #path is from pipelines.py file
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                self.STRING_TO_REPLACE[row[0]] = row[1] #loads each couple (key=original bet, value=formatted bet)
    
    def load_allowed_bets (self):
        self.ALLOWED_BETS = {}  #each bookmaker is a dict

        with open(f'.\\Resources\\{self.allowed_bets_filename}.csv') as csv_file:   #path is from pipelines.py file
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                self.ALLOWED_BETS[row[0]] = True


    def replace_bets(self, bet):
        if self.STRING_TO_REPLACE.get(bet):
            return self.STRING_TO_REPLACE[bet]
        else:
            return bet
        # bet_replaced = bet
        # if bet in self.STRING_TO_REPLACE.keys():
        #     bet_replaced = self.STRING_TO_REPLACE[bet]
        # return bet_replaced

    def is_allowed_bet(self, bet):
        return self.ALLOWED_BETS.get(bet)

    def fractional_to_decimal_odd(self, fraction):
        try:
             #if cast is not allowed, raise an exception, because the fraction must be converted
            return str(round(Decimal(fraction), 2))
        except (ValueError, InvalidOperation):
            if fraction == None:
                return '1'
            if fraction == 'Evens': #evens means a 2.0 resulting odd
                fraction = "1/1"
            num, denom = fraction.split('/')
            return str(round(Decimal(num) / Decimal(denom), 2) + 1)
        except TypeError:
            if fraction == None: 
                return '1'
            else:
                return fraction
    

        

    
        
            
        







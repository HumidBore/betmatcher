import pyodbc 
import time
import timeit
import pandas as pd
import re, csv
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
# server = 'tcp:betmatcher.database.windows.net' 
# database = 'betmatcher' 
# username = 'azureadmin' 
# password = 'xcazU7qpal3.' 
# driver= '{ODBC Driver 17 for SQL Server}'

# with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
#     with conn.cursor() as cursor:
#         df = pd.read_sql_query(
#                     '''select *
#                         from [dbo].[scraping_results]''', conn, index_col=['bookmaker','betRadarID'])
class TestComparatorFromAzureDB (object):
    server = 'tcp:betmatcher.database.windows.net' 
    database = 'betmatcher' 
    username = 'azureadmin' 
    password = 'xcazU7qpal3.' 
    driver= '{ODBC Driver 17 for SQL Server}'

    #TABLES
    SCRAPED_BETS_TABLE = "scraping_results"

    def __init__(self):
        self.CounterBetsTwoOutcomes = {} #key= bet , value = counterBet
        self.CounterBetsThreeOutcomes = {}   #key=bet , value = (counterBet1, counterBet2)
        
        self.load_opposite_bets()

        self.connection = pyodbc.connect(f'DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password}')
        self.cursor = self.connection.cursor()
        
        self.reset_all()

    def reset_all (self):
        self.BetMatches = []

        self.df = pd.read_sql_query(f'''SELECT * 
                        FROM {self.SCRAPED_BETS_TABLE}
                        WHERE bet in {self.bet_list_two_outcomes}''', self.connection)
        self.df_indexed = pd.read_sql_query(f'''SELECT * 
                        FROM {self.SCRAPED_BETS_TABLE}
                        WHERE bet in {self.bet_list_two_outcomes}''', self.connection, index_col=['bookmaker','betRadarID'])

    def load_opposite_bets (self):
        ###########  LOAD OPPOSITE TYPE OF BETS (TWO OUTCOMES)  ###########
        with open(f'.\\Resources\\CounterBetsTwoOutcomes.csv') as csv_file:   #path is from betmatcher outer folder
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                self.CounterBetsTwoOutcomes[row[0]] = row[1]
        
        ###########  LOAD OPPOSITE TYPE OF BETS (THREE OUTCOMES)  ###########
        with open(f'.\\Resources\\CounterBetsThreeOutcomes.csv') as csv_file:   #path is from betmatcher outer folder
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                self.CounterBetsThreeOutcomes[row[0]] = (row[1], row[2])

        self.bet_list_two_outcomes = self.convert_list_to_sql_list(self.CounterBetsTwoOutcomes.keys())
        self.bet_list_three_outcomes = self.convert_list_to_sql_list(self.CounterBetsThreeOutcomes.keys())
        

    def convert_list_to_sql_list (self, bet_dict_keys):   #must be invoked passing bets_dict.keys()
        #creates a list of bets with two outcomes. It will be formatted and used inside the query to extract all the bets inside this list
        sql_bet_list = str(list(bet_dict_keys))

        #string formatting to crate a list for sqlite (a list in sqlite is like (...))
        sql_bet_list = re.sub(r'\]{1,1}$', ')', re.sub(r'^\[?', '(', sql_bet_list))
        return sql_bet_list

    def find_matches(self):
        self.find_matches_with_two_outcomes()

        self.cursor.close()
        self.connection.close()
        print("The Sqlite connection is closed")

        
    def find_matches_with_two_outcomes(self):
        
        

        start_finding_outcomes = time.time()
        for row in self.df.head(1000).itertuples():
            self.find_opposite_bet_two_outcomes(row)
        END = time.time()
        print("PRIMI 1000, TEMPO TRASCORSO:", (END-start_finding_outcomes))
        exit()


        self.BetMatches.clear()

    def find_opposite_bet_two_outcomes(self, row):
        opposite_odd = self.df_indexed[(self.df_indexed.index.get_level_values('bookmaker') != row[1]) & 
                (self.df_indexed.index.get_level_values('betRadarID') == row[3]) &
                (self.df_indexed['bet'] == row[6])
                        ]

        # find_opposite_bet_two_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
        #                 FROM {self.SCRAPED_BETS_TABLE}
        #                 WHERE  betRadarID = {row[2]}
        #                     and bet = '{self.CounterBetsTwoOutcomes.get(row[5])}' 
        #                     and bookmaker != '{row[0]}'
        #                 """
            
        # self.cursor.execute(find_opposite_bet_two_outcomes_query)
        # opposite_odd = self.cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)
        for odd in opposite_odd.itertuples():    #more than one bookmaker have the same bets
                # self.BetMatches.append((self.compute_rtp(float(row[6]), float(odd[2])), #appends data to the record list
                #                         row[1],    #row[1] is tournament
                #                         row[2],    #row[2] is betRadarID
                #                         row[3],    #row[3] is event
                #                         row[4],    #row[4] is date
                #                         row[0],    #row[0] is bookmaker1
                #                         row[5],    #row[5] is bet1
                #                         row[6],    #row[6] is betOdd1
                #                         odd[0],    #odd[0] is bookmaker2
                #                         odd[1],    #odd[1] is bet2
                #                         odd[2]     #odd[2] is betOdd2
                #                         ))
            self.BetMatches.append(odd)  
        
        

    # def find_matches_with_three_outcomes(self):
    #     select_bets_with_three_outcomes_in_list_query = f"""SELECT * 
    #                                                 FROM {self.SCRAPED_BETS_TABLE}
    #                                                 WHERE bet in {self.bet_list_three_outcomes}
    #                                             """
    #     self.cursor.execute(select_bets_with_three_outcomes_in_list_query)
    #     records = self.cursor.fetchall()

    #     for row in records: #for each bet, finds the opposite ones 
    #         self.find_opposite_bet_three_outcomes(row)

    #     self.cursor.executemany(f'''
    #              INSERT INTO {self.BET_MATCHES_THREE_OUTCOMES_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    #          ''', self.BetMatches)

    #     #create an index on rtp column to speed up ordering
    #     self.cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE)

    #     self.connection.commit()
    #     self.BetMatches = []

    # def find_opposite_bet_three_outcomes(self, row):
    #     #query to get the second outcome
    #     find_opposite_bet_2_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd, betRadarID
    #                     FROM {self.SCRAPED_BETS_TABLE}
    #                     WHERE  ((betRadarID != -1 and betRadarID = "{row[2]}") OR (event = "{row[3]}" and date = "{row[4]}")) 
    #                         and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[0]}" and bookmaker != "{row[0]}"
    #                     """
    #     self.cursor.execute(find_opposite_bet_2_three_outcomes_query)
        
    #     opposite_odd_2 = self.cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)

    #     if opposite_odd_2:
    #         #query to get the third outcome
    #         find_opposite_bet_3_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
    #                         FROM {self.SCRAPED_BETS_TABLE}
    #                         WHERE  ((betRadarID != -1 and (betRadarID = "{row[2]}" or betRadarID = "{opposite_odd_2[0][3]}")) OR (event = "{row[3]}" and date = "{row[4]}")) 
    #                             and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[1]}" and  bookmaker != "{row[0]}"
    #                         """ 
    #         self.cursor.execute(find_opposite_bet_3_three_outcomes_query)

    #         opposite_odd_3 = self.cursor.fetchall() 

    #         for odd2 in opposite_odd_2:    #more than one bookmaker have the same bets
    #             for odd3 in opposite_odd_3:
    #                 if odd2[0] != odd3[0]:  #check if the third bookmaker is different from the second one 
    #                     self.BetMatches.append((self.compute_rtp(float(row[6]), float(odd2[2]), float(odd3[2])), #appends data to the record list
    #                                             row[1],    #row[1] is tournament
    #                                             row[2],    #row[2] is betRadarID
    #                                             row[3],    #row[3] is event
    #                                             row[4],    #row[4] is date
    #                                             row[0],    #row[0] is bookmaker1
    #                                             row[5],    #row[5] is bet1
    #                                             row[6],    #row[6] is betOdd1
    #                                             odd2[0],    #odd2[0] is bookmaker2
    #                                             odd2[1],    #odd2[1] is bet2
    #                                             odd2[2],    #odd2[2] is betOdd2
    #                                             odd3[0],    #odd3[0] is bookmaker3
    #                                             odd3[1],    #odd3[1] is bet3
    #                                             odd3[2],    #odd3[2] is betOdd3
    #                                             )) 
                  

    def compute_rtp(self,firstOdd, secondOdd, *args):   #computes rtp based on odds. *args is because this method can be invoke with two or thre bets
        if not args:
            result = (firstOdd * secondOdd) / (firstOdd + secondOdd) * 100
        else:   # args[0] is the third odd
            result = (firstOdd * secondOdd * args[0]) / (firstOdd * secondOdd + secondOdd * args[0] + firstOdd * args[0]) * 100
        return str(round(result, 2))

comparator = TestComparatorFromAzureDB()
comparator.find_matches()
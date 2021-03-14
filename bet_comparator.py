import sqlite3
import csv
import re
import os
import time
import sys


class BetComparator (object):

    PATH = '.\\Databases\\'     #relative path to the db (.\\ stands for betmatcher outer folder)
    DB_NAME = "TESTbetmatcher.db"

    #TABLES
    SCRAPED_BETS_TABLE = "test_multiple_spiders"
    COPY_SCRAPED_BETS_TABLE = "copy_" + SCRAPED_BETS_TABLE
    BET_MATCHES_TWO_OUTCOMES_TABLE = "bet_matches_two_outcomes"
    BET_MATCHES_THREE_OUTCOMES_TABLE = "bet_matches_three_outcomes"

    #INDEXES
    INDEX_COPY_SCRAPED_BETS_TABLE = "betRadarID_bet_bookmaker_index_copy_table"
    INDEX_2_COPY_SCRAPED_BETS_TABLE = "event_date_bet_bookmaker_index_copy_table"
    INDEX_BET_MATCHES_TWO_OUTCOMES_TABLE = "rtp_index_two_outcomes_table"
    INDEX_BET_MATCHES_THREE_OUTCOMES_TABLE = "rtp_index_three_outcomes_table"

    #QUERIES
    DROP_TABLE_COPY_SCRAPED_BETS = f'DROP TABLE IF EXISTS {COPY_SCRAPED_BETS_TABLE}'

    DROP_TABLE_BET_MATCHES_TWO_OUTCOMES_TABLE = f'DROP TABLE IF EXISTS {BET_MATCHES_TWO_OUTCOMES_TABLE}'
    DROP_TABLE_BET_MATCHES_THREE_OUTCOMES_TABLE = f'DROP TABLE IF EXISTS {BET_MATCHES_THREE_OUTCOMES_TABLE}'

    CREATE_TABLE_COPY_SCRAPED_BETS = f'''CREATE TABLE IF NOT EXISTS {COPY_SCRAPED_BETS_TABLE} (
                                    bookmaker TEXT,
                                    tournament TEXT,
                                    betRadarID INT,
                                    event TEXT,
                                    date TEXT,
                                    bet TEXT,
                                    betOdd REAL,
                                    PRIMARY KEY(bookmaker,betRadarID,bet)
                                    )
                                '''
    COPY_RECORDS_INTO_COPY_SCRAPED_BETS_TABLE = f"INSERT INTO {COPY_SCRAPED_BETS_TABLE} select * from {SCRAPED_BETS_TABLE}"

    # DROP_INDEX_COPY_SCRAPED_BET_TABLE = f'DROP INDEX IF EXISTS {INDEX_COPY_SCRAPED_BETS_TABLE}'

    CREATE_TABLE_BET_MATCHES_TWO_OUTCOMES = f''' CREATE TABLE IF NOT EXISTS {BET_MATCHES_TWO_OUTCOMES_TABLE}(
                                    RTP REAL,
                                    tournament TEXT,
                                    betRadarID INT,
                                    event TEXT,
                                    date TEXT,
                                    bookmaker1 TEXT,
                                    bet1 TEXT,
                                    betOdd1 REAL,
                                    bookmaker2 TEXT,
                                    bet2 TEXT,
                                    betOdd2 REAL,
                                    PRIMARY KEY(betRadarID,event,bookmaker1,bet1,bookmaker2,bet2)
                                )
                            '''
    CREATE_TABLE_BET_MATCHES_THREE_OUTCOMES = f''' CREATE TABLE IF NOT EXISTS {BET_MATCHES_THREE_OUTCOMES_TABLE}(
                                    RTP REAL,
                                    tournament TEXT,
                                    betRadarID INT,
                                    event TEXT,
                                    date TEXT,
                                    bookmaker1 TEXT,
                                    bet1 TEXT,
                                    betOdd1 REAL,
                                    bookmaker2 TEXT,
                                    bet2 TEXT,
                                    betOdd2 REAL,
                                    bookmaker3 TEXT,
                                    bet3 TEXT,
                                    betOdd3 REAL,
                                    PRIMARY KEY(betRadarID,event,bookmaker1,bet1,bookmaker2,bet2,bookmaker3,bet3)
                                )
                            '''

    CREATE_INDEX_ON_COPY_TABLE = f'CREATE INDEX {INDEX_COPY_SCRAPED_BETS_TABLE} ON {COPY_SCRAPED_BETS_TABLE}(betRadarID,bet,bookmaker)'
    
    CREATE_INDEX_2_ON_COPY_TABLE = f'CREATE INDEX {INDEX_2_COPY_SCRAPED_BETS_TABLE} ON {COPY_SCRAPED_BETS_TABLE}(event,date,bet,bookmaker)'
    
    CREATE_INDEX_ON_BET_MATCHES_TWO_OUTCOMES_TABLE = f'CREATE INDEX {INDEX_BET_MATCHES_TWO_OUTCOMES_TABLE} ON {BET_MATCHES_TWO_OUTCOMES_TABLE}(rtp)'
    
    CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE = f'CREATE INDEX {INDEX_BET_MATCHES_THREE_OUTCOMES_TABLE} ON {BET_MATCHES_THREE_OUTCOMES_TABLE}(rtp)'


    def __init__(self):
        self.CounterBetsTwoOutcomes = {} #key= bet , value = counterBet
        self.CounterBetsThreeOutcomes = {}   #key=bet , value = (counterBet1, counterBet2)
        

        self.load_opposite_bets()

        self.connection = sqlite3.connect(self.PATH + self.DB_NAME)    #connect to the database specified as string (.db is the extension for databases in sqllite3)
        self.cursor = self.connection.cursor()
        self.reset_all()

    def reset_all (self):
        self.BetMatches = []

        self.cursor.execute(self.DROP_TABLE_COPY_SCRAPED_BETS)
        self.cursor.execute(self.DROP_TABLE_BET_MATCHES_TWO_OUTCOMES_TABLE)
        self.cursor.execute(self.DROP_TABLE_BET_MATCHES_THREE_OUTCOMES_TABLE)
        
        self.cursor.execute(self.CREATE_TABLE_COPY_SCRAPED_BETS)
        self.cursor.execute(self.COPY_RECORDS_INTO_COPY_SCRAPED_BETS_TABLE)   #copies all the elements inside the new table
        self.cursor.execute(self.CREATE_TABLE_BET_MATCHES_TWO_OUTCOMES)
        self.cursor.execute(self.CREATE_TABLE_BET_MATCHES_THREE_OUTCOMES)

        self.cursor.execute(self.CREATE_INDEX_ON_COPY_TABLE) #creates an index on betRadarID,bet,bookmaker
        self.cursor.execute(self.CREATE_INDEX_2_ON_COPY_TABLE) #creates an index on event,bet,bookmaker
        
        self.connection.commit()

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

        self.bet_list_two_outcomes = self.convert_list_to_sqlite_list(self.CounterBetsTwoOutcomes.keys())
        self.bet_list_three_outcomes = self.convert_list_to_sqlite_list(self.CounterBetsThreeOutcomes.keys())


    def convert_list_to_sqlite_list (self, bet_dict_keys):   #must be invoked passing bets_dict.keys()
        #creates a list of bets with two outcomes. It will be formatted and used inside the query to extract all the bets inside this list
        sqlite_bet_list = str(list(bet_dict_keys))

        #string formatting to crate a list for sqlite (a list in sqlite is like (...))
        sqlite_bet_list = re.sub(r'\]{1,1}$', ')', re.sub(r'\'', '\"', re.sub(r'^\[?', '(', sqlite_bet_list)))
        return sqlite_bet_list

    def find_matches(self):
        self.find_matches_with_two_outcomes()

        

        self.cursor.close()
        self.connection.close()
        print("The Sqlite connection is closed")

    
    def find_matches_with_two_outcomes(self):
        select_bets_with_two_outcomes_in_list_query = f"""SELECT * 
                                                    from {self.COPY_SCRAPED_BETS_TABLE}
                                                    where bet in {self.bet_list_two_outcomes}
                                                """
        self.cursor.execute(select_bets_with_two_outcomes_in_list_query)
        records = self.cursor.fetchall()

        for row in records:
            self.find_opposite_bet_two_outcomes(row)

        self.cursor.executemany(f'''
                 INSERT OR REPLACE INTO {self.BET_MATCHES_TWO_OUTCOMES_TABLE} (RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2) VALUES(?,?,?,?,?,?,?,?,?,?,?)
             ''', self.BetMatches)
        self.cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_TWO_OUTCOMES_TABLE)

        self.connection.commit()

        self.BetMatches = []

    def find_opposite_bet_two_outcomes(self, row):
        find_opposite_bet_two_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
                        from {self.COPY_SCRAPED_BETS_TABLE}
                        WHERE  ((betRadarID not NULL and betRadarID = "{row[2]}") OR (event = "{row[3]}" and date = "{row[4]}")) 
                        and bet = "{self.CounterBetsTwoOutcomes.get(row[5])}" and  bookmaker != "{row[0]}"
                        """
        self.cursor.execute(find_opposite_bet_two_outcomes_query)
        
        opposite_odd = self.cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)
        
        if opposite_odd:
            for odd in opposite_odd:    #more than one bookmaker have the same bets
                self.BetMatches.append((self.compute_rtp(float(row[6]), float(odd[2])), #appends data to the record list
                                        row[1],    #row[1] is tournament
                                        row[2],    #row[2] is betRadarID
                                        row[3],    #row[3] is event
                                        row[4],    #row[4] is date
                                        row[0],    #row[0] is bookmaker1
                                        row[5],    #row[5] is bet1
                                        row[6],    #row[6] is betOdd1
                                        odd[0],    #odd[0] is bookmaker2
                                        odd[1],    #odd[1] is bet2
                                        odd[2]     #odd[2] is betOdd2
                                        ))  

    def find_matches_with_three_outcomes(self):
        select_bets_with_three_outcomes_in_list_query = f"""SELECT * 
                                                    from {self.COPY_SCRAPED_BETS_TABLE}
                                                    where bet in {self.bet_list_three_outcomes}
                                                """
        self.cursor.execute(select_bets_with_three_outcomes_in_list_query)
        records = self.cursor.fetchall()

        for row in records: #for each bet, finds the opposite ones 
            self.find_opposite_bet_three_outcomes(row)

        self.cursor.executemany(f'''
                 INSERT OR REPLACE INTO {self.BET_MATCHES_THREE_OUTCOMES_TABLE} (RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,bookmaker3,bet3,betOdd3) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             ''', self.BetMatches)

        #create an index on rtp column to speed up ordering
        self.cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE)

        self.connection.commit()
        self.BetMatches = []

    def find_opposite_bet_three_outcomes(self, row):
        #query to get the second outcome
        find_opposite_bet_2_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd, betRadarID
                        from {self.COPY_SCRAPED_BETS_TABLE}
                        WHERE  ((betRadarID not NULL and betRadarID = "{row[2]}") OR (event = "{row[3]}" and date = "{row[4]}")) 
                        and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[0]}" and  bookmaker != "{row[0]}"
                        """
        self.cursor.execute(find_opposite_bet_2_three_outcomes_query)
        
        opposite_odd_2 = self.cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)

        if opposite_odd_2:
            #query to get the third outcome
            find_opposite_bet_3_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
                            from {self.COPY_SCRAPED_BETS_TABLE}
                            WHERE  ((betRadarID not NULL and (betRadarID = "{row[2]}" or betRadarID = "{opposite_odd_2[0][3]}")) OR (event = "{row[3]}" and date = "{row[4]}")) 
                            and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[1]}" and  bookmaker != "{row[0]}"
                            """ 
            self.cursor.execute(find_opposite_bet_3_three_outcomes_query)

            opposite_odd_3 = self.cursor.fetchall() 

            for odd2 in opposite_odd_2:    #more than one bookmaker have the same bets
                for odd3 in opposite_odd_3:
                    if odd2[0] != odd3[0]:  #check if the third bookmaker is different from the second one 
                        self.BetMatches.append((self.compute_rtp(float(row[6]), float(odd2[2]), float(odd3[2])), #appends data to the record list
                                                row[1],    #row[1] is tournament
                                                row[2],    #row[2] is betRadarID
                                                row[3],    #row[3] is event
                                                row[4],    #row[4] is date
                                                row[0],    #row[0] is bookmaker1
                                                row[5],    #row[5] is bet1
                                                row[6],    #row[6] is betOdd1
                                                odd2[0],    #odd2[0] is bookmaker2
                                                odd2[1],    #odd2[1] is bet2
                                                odd2[2],    #odd2[2] is betOdd2
                                                odd3[0],    #odd3[0] is bookmaker3
                                                odd3[1],    #odd3[1] is bet3
                                                odd3[2],    #odd3[2] is betOdd3
                                                )) 
                  

    def compute_rtp(self,firstOdd, secondOdd, *args):   #computes rtp based on odds. *args is because this method can be invoke with two or thre bets
        if not args:
            result = (firstOdd * secondOdd) / (firstOdd + secondOdd) * 100
        else:   # args[0] is the third odd
            result = (firstOdd * secondOdd * args[0]) / (firstOdd * secondOdd + secondOdd * args[0] + firstOdd * args[0]) * 100
        return str(round(result, 2))

start = time.time()
comparator = BetComparator()
comparator.find_matches()
end = time.time()
print("TEMPO TRASCORSO: ", (end - start) / 60.0, 'minuti')

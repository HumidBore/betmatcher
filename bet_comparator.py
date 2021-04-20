import sqlite3
import csv
import re

import time
from datetime import datetime, timedelta

import pyodbc 

from decimal import Decimal


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
                        WHERE  ((betRadarID != -1 and betRadarID = "{row[2]}") OR (event = "{row[3]}" and date = "{row[4]}")) 
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
                        WHERE  ((betRadarID != -1 and betRadarID = "{row[2]}") OR (event = "{row[3]}" and date = "{row[4]}")) 
                        and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[0]}" and  bookmaker != "{row[0]}"
                        """
        self.cursor.execute(find_opposite_bet_2_three_outcomes_query)
        
        opposite_odd_2 = self.cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)

        if opposite_odd_2:
            #query to get the third outcome
            find_opposite_bet_3_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
                            from {self.COPY_SCRAPED_BETS_TABLE}
                            WHERE  ((betRadarID != -1 and (betRadarID = "{row[2]}" or betRadarID = "{opposite_odd_2[0][3]}")) OR (event = "{row[3]}" and date = "{row[4]}")) 
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





class BetComparatorFromAzureDB (object):
    #GENERAL SETTINGS
    UPLOAD_TO_AZURE_RECORDS_LIMIT = 1000 #number of rows to iter before committing

    #AZURE DB SETTINGS TO CONNECT
    server = 'tcp:betmatcher.database.windows.net' 
    database = 'betmatcher' 
    username = 'azureadmin' 
    password = 'xcazU7qpal3.' 
    driver= '{ODBC Driver 17 for SQL Server}'

    params = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

    # SQLITE DB SETTINGS TO CONNECT
    PATH = '.\\Databases\\'     #relative path to the db (.\\ stands for betmatcher outer folder)
    DB_NAME = "TESTbetmatcher.db"

    #AZURE TABLES
    AZURE_SCRAPED_BETS_TABLE = "scraping_results"
    BET_MATCHES_TWO_OUTCOMES_TABLE = "bet_matches_two_outcomes"
    BET_MATCHES_THREE_OUTCOMES_TABLE = "bet_matches_three_outcomes"

    #INDEXES AZURE
    # IX_BetRadarID_Bet_Bookmaker = "IX_BetRadarID_Bet_Bookmaker"
    IX_Rtp_two_outcomes_table = "IX_Rtp_two_outcomes_table"
    IX_Rtp_three_outcomes_table = "IX_Rtp_three_outcomes_table"

    #AZURE DATABASE QUERIES
    TRUNCATE_AZURE_TABLE_SCRAPING_RESULTS = f'TRUNCATE TABLE {AZURE_SCRAPED_BETS_TABLE}'

    DROP_TABLE_BET_MATCHES_TWO_OUTCOMES_TABLE = f'DROP TABLE IF EXISTS {BET_MATCHES_TWO_OUTCOMES_TABLE}'
    DROP_TABLE_BET_MATCHES_THREE_OUTCOMES_TABLE = f'DROP TABLE IF EXISTS {BET_MATCHES_THREE_OUTCOMES_TABLE}'

    CREATE_TABLE_BET_MATCHES_TWO_OUTCOMES = f'''CREATE TABLE {BET_MATCHES_TWO_OUTCOMES_TABLE} (
                            RTP DECIMAL(5,2),
                            tournament varchar(80),
                            betRadarID int,
                            event varchar(120),
                            date datetime2(0),
                            bookmaker1 varchar(20),
                            bet1 varchar(20),
                            betOdd1 DECIMAL(5,2),
                            bookmaker2 varchar(20),
                            bet2 varchar(20),
                            betOdd2 DECIMAL(5,2),  
                            insertingdate datetime2(0),
                            CONSTRAINT PK_{BET_MATCHES_TWO_OUTCOMES_TABLE} PRIMARY KEY NONCLUSTERED (betRadarID,bookmaker1,bet1,bookmaker2,bet2)
                        )
                    '''
    # CONSTRAINT PK_{BET_MATCHES_TWO_OUTCOMES_TABLE} PRIMARY KEY NONCLUSTERED (betRadarID,bookmaker1,bet1,bookmaker2,bet2)                            
    CREATE_TABLE_BET_MATCHES_THREE_OUTCOMES = f'''CREATE TABLE {BET_MATCHES_THREE_OUTCOMES_TABLE}(
                                    RTP DECIMAL(5,2),
                                    tournament varchar(80),
                                    betRadarID int,
                                    event varchar(120),
                                    date datetime2(0),
                                    bookmaker1 varchar(20),
                                    bet1 varchar(20),
                                    betOdd1 DECIMAL(5,2),
                                    bookmaker2 varchar(20),
                                    bet2 varchar(20),
                                    betOdd2 DECIMAL(5,2),
                                    bookmaker3 varchar(20),
                                    bet3 varchar(20),
                                    betOdd3 DECIMAL(5,2),
                                    insertingdate datetime2(0),
                                    CONSTRAINT PK_{BET_MATCHES_THREE_OUTCOMES_TABLE} PRIMARY KEY NONCLUSTERED (betRadarID,bookmaker1,bet1,bookmaker2,bet2,bookmaker3,bet3)
                                )
                            '''
    
                                    # CONSTRAINT PK_{BET_MATCHES_THREE_OUTCOMES_TABLE} PRIMARY KEY NONCLUSTERED (betRadarID,bookmaker1,bet1,bookmaker2,bet2,bookmaker3,bet3)
    # CREATE_INDEX_ON_SCRAPING_RESULTS_TABLE = f'CREATE NONCLUSTERED INDEX {IX_BetRadarID_Bet_Bookmaker} ON {SCRAPED_BETS_TABLE} ([betRadarID],[bet],[bookmaker]) INCLUDE ([betOdd])'

    CREATE_INDEX_ON_BET_MATCHES_TWO_OUTCOMES_TABLE = f'CREATE NONCLUSTERED INDEX {IX_Rtp_two_outcomes_table} ON {BET_MATCHES_TWO_OUTCOMES_TABLE} (RTP)'
    
    CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE = f'CREATE NONCLUSTERED INDEX {IX_Rtp_three_outcomes_table} ON {BET_MATCHES_THREE_OUTCOMES_TABLE} (RTP)'

    SQL_MERGE_TWO_OUTCOMES =  """
            MERGE INTO [dbo].[bet_matches_two_outcomes] AS results
            USING (VALUES (?,?,?,?,?,?,?,?,?,?,?,?)) AS newval(RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,insertingdate)
                    ON results.betRadarID = newval.betRadarID and
                    results.bookmaker1 = newval.bookmaker1 and
                    results.bet1 = newval.bet1 and
                    results.bookmaker2 = newval.bookmaker2 and
                    results.bet2 = newval.bet2
            WHEN MATCHED THEN
                    UPDATE SET results.RTP = newval.RTP, results.betOdd1 = newval.betOdd1, results.betOdd2 = newval.betOdd2, results.insertingdate = newval.insertingdate
            WHEN NOT MATCHED THEN
                    INSERT (RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,insertingdate) 
                    VALUES (newval.RTP, newval.tournament, newval.betRadarID, newval.event, newval.date, newval.bookmaker1, newval.bet1, newval.betOdd1,newval.bookmaker2, newval.bet2, newval.betOdd2,newval.insertingdate);
            """ 
    SQL_MERGE_THREE_OUTCOMES =  """
            MERGE INTO [dbo].[bet_matches_three_outcomes] AS results
            USING (VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)) AS newval(RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,bookmaker3,bet3,betOdd3,insertingdate)
                    ON results.betRadarID = newval.betRadarID and
                    results.bookmaker1 = newval.bookmaker1 and
                    results.bet1 = newval.bet1 and
                    results.bookmaker2 = newval.bookmaker2 and
                    results.bet2 = newval.bet2 and
                    results.bookmaker3 = newval.bookmaker3 and
                    results.bet3 = newval.bet3
            WHEN MATCHED THEN
                    UPDATE SET results.RTP = newval.RTP, results.betOdd1 = newval.betOdd1, results.betOdd2 = newval.betOdd2,results.betOdd3 = newval.betOdd3, results.insertingdate = newval.insertingdate
            WHEN NOT MATCHED THEN
                    INSERT (RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,bookmaker3,bet3,betOdd3,insertingdate) 
                    VALUES (newval.RTP, newval.tournament, newval.betRadarID, newval.event, newval.date, newval.bookmaker1, newval.bet1, newval.betOdd1,newval.bookmaker2, newval.bet2, newval.betOdd2,newval.bookmaker3, newval.bet3, newval.betOdd3,newval.insertingdate);
            """ 

    ######  SQLITE #####

    #SQLITE TABLES
    SQLITE_SCRAPED_BETS_TABLE = "ScrapingResultsSqlite"

    #INDEX SQLITE
    INDEX_SQLITE_BetRadarID_Bet_Bookmaker = "betRadarID_bet_bookmaker_index"

    # SQLITE QUERIES
    CREATE_SQLITE_SCRAPED_BETS_TABLE = f'''CREATE TABLE IF NOT EXISTS {SQLITE_SCRAPED_BETS_TABLE} (
                                    bookmaker TEXT,
                                    tournament TEXT,
                                    betRadarID INT,
                                    event TEXT,
                                    date DATE,
                                    bet TEXT,
                                    betOdd TEXT,
                                    PRIMARY KEY(bookmaker,betRadarID,bet)
                                    )
                                '''
    DROP_SCRAPED_BETS_SQLITE_TABLE = f"DROP TABLE IF EXISTS {SQLITE_SCRAPED_BETS_TABLE}"
    
    CREATE_INDEX_ON_SQLITE_SCRAPED_BETS_TABLE = f'CREATE INDEX {INDEX_SQLITE_BetRadarID_Bet_Bookmaker} ON {SQLITE_SCRAPED_BETS_TABLE}(betRadarID,bet,bookmaker)'
    
    def __init__(self):
        self.CounterBetsTwoOutcomes = {} #key= bet , value = counterBet
        self.CounterBetsThreeOutcomes = {}   #key=bet , value = (counterBet1, counterBet2)

        self.load_opposite_bets()

        self.azure_connection = pyodbc.connect(self.params)
        self.azure_cursor = self.azure_connection.cursor()
        self.azure_cursor.fast_executemany = True   #speeds up insert statements

        self.sqlite_connection = sqlite3.connect(self.PATH + self.DB_NAME)    #connect to the database specified as string (.db is the extension for databases in sqllite3)
        self.sqlite_cursor = self.sqlite_connection.cursor()

        self.reset_all()
    
    
    def reset_all (self):
        self.BetMatches = []    #list of rows created matching events

        #AZURE
        # self.azure_cursor.execute(self.DROP_TABLE_BET_MATCHES_TWO_OUTCOMES_TABLE)
        # self.azure_cursor.execute(self.DROP_TABLE_BET_MATCHES_THREE_OUTCOMES_TABLE)

        # self.azure_cursor.execute(self.CREATE_TABLE_BET_MATCHES_TWO_OUTCOMES)
        # self.azure_cursor.execute(self.CREATE_TABLE_BET_MATCHES_THREE_OUTCOMES)

        # self.azure_cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_TWO_OUTCOMES_TABLE)
        # self.azure_cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE)

        self.azure_cursor.fast_executemany = True

        self.try_to_find_betRadarID()
        
        self.azure_connection.commit()

        #SQLITE
        self.drop_and_create_table_sqlite()

        self.sqlite_connection.commit()

    
    def try_to_find_betRadarID (self):
        #Tries to find a betRadarID for the events which do not have one
        self.azure_cursor.execute(f"""UPDATE  a
                                    SET     a.betRadarID = b.betRadarID	
                                    FROM    {self.AZURE_SCRAPED_BETS_TABLE} a INNER JOIN {self.AZURE_SCRAPED_BETS_TABLE} b
                                                                    ON a.event = b.event and a.date = b.date
                                    WHERE a.betRadarID = -1 and b.betRadarID != -1
                                    """)

        #Deletes all the elements without a betRadarID (no match would be possible, they are only junk rows)
        self.azure_cursor.execute(f"""DELETE 
                                    FROM scraping_results
                                    WHERE betRadarID = -1
                                    """)
    def drop_and_create_table_sqlite(self):
        self.sqlite_cursor.execute(self.DROP_SCRAPED_BETS_SQLITE_TABLE)
        self.sqlite_cursor.execute(self.CREATE_SQLITE_SCRAPED_BETS_TABLE)

    def copy_azure_table_in_sqlite(self, list_of_bets):   #prendi i dati da azure e li copia in sqlite, selezionando le righe con le bet specificate
        self.azure_cursor.execute(f"""SELECT * 
                                        FROM {self.AZURE_SCRAPED_BETS_TABLE}
                                        WHERE bet in {list_of_bets}
                                    """)
       
        self.sqlite_cursor.executemany(f'''
                INSERT INTO {self.SQLITE_SCRAPED_BETS_TABLE} (bookmaker,tournament,betRadarID,event,date,bet,betOdd) VALUES(?,?,?,?,?,?,?)
            ''', self.azure_cursor.fetchall())

        self.sqlite_cursor.execute(self.CREATE_INDEX_ON_SQLITE_SCRAPED_BETS_TABLE)

        self.sqlite_connection.commit()

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

        self.bet_list_two_outcomes = self.convert_list_to_sql_server_list(self.CounterBetsTwoOutcomes.keys())    
        self.bet_list_three_outcomes = self.convert_list_to_sql_server_list(self.CounterBetsThreeOutcomes.keys())
        
    def convert_list_to_sqlite_list (self, bet_dict_keys):   #must be invoked passing bet_dict.keys()
        #creates a list of bets with two outcomes. It will be formatted and used inside the query to extract all the bets inside this list
        sqlite_bet_list = str(list(bet_dict_keys))

        #string formatting to crate a list for sqlite (a list in sqlite is like (...))
        sqlite_bet_list = re.sub(r'\]{1,1}$', ')', re.sub(r'\'', '\"', re.sub(r'^\[?', '(', sqlite_bet_list)))
        return sqlite_bet_list

    def convert_list_to_sql_server_list (self, bet_dict_keys):   #must be invoked passing bet_dict.keys()
        #creates a list of bets with two outcomes. It will be formatted and used inside the query to extract all the bets inside this list
        sql_bet_list = str(list(bet_dict_keys))

        #string formatting to crate a list for sqlite (a list in sqlite is like (...))
        sql_bet_list = re.sub(r'\]{1,1}$', ')', re.sub(r'^\[?', '(', sql_bet_list))
        return sql_bet_list

    def find_matches(self):
        self.inserting_date_and_time = datetime.now()  #minus 2 seconds because sql time precision is at the second
        self.deleting_date_and_time = self.inserting_date_and_time-timedelta(seconds=2)  #minus 2 seconds because sql time precision is at the second
        print("DELITING DATE", self.deleting_date_and_time)
        print("INSERTING DATE", self.inserting_date_and_time)
        #fills sqlite table with rows with two outcomes
        self.copy_azure_table_in_sqlite(self.bet_list_two_outcomes)
        self.find_matches_with_two_outcomes()

        #empties the table, because now three outcomes bets are needed, and fills sqlite table with rows with three outcomes
        self.drop_and_create_table_sqlite() 
        self.copy_azure_table_in_sqlite(self.bet_list_three_outcomes)

        self.find_matches_with_three_outcomes()

        self.sqlite_cursor.execute(self.DROP_SCRAPED_BETS_SQLITE_TABLE)

        self.azure_cursor.execute(self.TRUNCATE_AZURE_TABLE_SCRAPING_RESULTS)
        self.azure_connection.commit()

        self.azure_cursor.close()
        self.azure_connection.close()

        self.sqlite_cursor.close()
        self.sqlite_connection.close()

    def find_matches_with_two_outcomes(self):
        select_bets_with_two_outcomes_in_list_query = f"""SELECT * 
                                                        FROM {self.SQLITE_SCRAPED_BETS_TABLE}
                                                """
        self.sqlite_cursor.execute(select_bets_with_two_outcomes_in_list_query)
        records = self.sqlite_cursor.fetchall()

        start_finding_outcomes = time.time()

        # try:
        for row in records:
            self.find_opposite_bet_two_outcomes(row)
            if len(self.BetMatches) > self.UPLOAD_TO_AZURE_RECORDS_LIMIT:  #sends data to the server every 1000 rows
                # self.azure_cursor.executemany(f'''
                #  INSERT INTO {self.BET_MATCHES_TWO_OUTCOMES_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                #     ''', self.BetMatches)
                self.azure_cursor.executemany(self.SQL_MERGE_TWO_OUTCOMES, self.BetMatches)
                self.BetMatches.clear()  
        # except pyodbc.ProgrammingError as er:
        #     print(er)
        #     try:
        #         for i in self.BetMatches:
        #             self.azure_cursor.execute(self.SQL_MERGE_TWO_OUTCOMES, i)
        #     except pyodbc.ProgrammingError as e:
        #         print(e)

        END = time.time()
        print("TEMPO MATCHING EVENTI A DUE ESITI:", (END-start_finding_outcomes))
        
        if (self.BetMatches):  #sends last data 
            # self.azure_cursor.executemany(f'''INSERT INTO {self.BET_MATCHES_TWO_OUTCOMES_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            #     ''', self.BetMatches)
            self.azure_cursor.executemany(self.SQL_MERGE_TWO_OUTCOMES, self.BetMatches)

        #elimino le righe non aggiornate, perchè sono di eventi terminati, sospesi o cancellati
        self.azure_cursor.execute(f"""delete from {self.BET_MATCHES_TWO_OUTCOMES_TABLE} where insertingdate < '{self.deleting_date_and_time}' """)
        
        # start_index = time.time()
        # self.azure_cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_TWO_OUTCOMES_TABLE)
        # end_index = time.time()
        # print("TEMPO INDICE:", (end_index-start_index))
        self.BetMatches.clear()
        self.azure_connection.commit()
        

    def find_opposite_bet_two_outcomes(self, row):
        find_opposite_bet_two_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
                        from {self.SQLITE_SCRAPED_BETS_TABLE}
                        WHERE  betRadarID = "{row[2]}" and bet = "{self.CounterBetsTwoOutcomes.get(row[5])}" and  bookmaker != "{row[0]}"
                        """
        self.sqlite_cursor.execute(find_opposite_bet_two_outcomes_query)
        
        opposite_odd = self.sqlite_cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)
        
        if opposite_odd:
            for odd in opposite_odd:    #more than one bookmaker have the same bets
                self.BetMatches.append((self.compute_rtp_two_outcomes(float(row[6]), float(odd[2])), #appends data to the record list
                                        row[1],    #row[1] is tournament
                                        row[2],    #row[2] is betRadarID
                                        row[3],    #row[3] is event
                                        row[4],    #row[4] is date
                                        row[0],    #row[0] is bookmaker1
                                        re.sub(r'^.*? <> ', '', row[5]),    #row[5] is bet1
                                        # self.decimal_context.create_decimal_from_float(round(float(row[6]), 2)),    #row[6] is betOdd1, stored as text in sqlite, then cast to float, rounded to 2 decimal digits and cast to Decimal to insert in azure sql  
                                        round(Decimal(row[6]), 2),    #row[6] is betOdd1, stored as text in sqlite, then cast to Decimal, rounded to 2 decimal digits and cast to Decimal to insert in azure sql  
                                        odd[0],    #odd[0] is bookmaker2
                                        re.sub(r'^.*? <> ', '', odd[1]),    #odd[1] is bet2
                                        # self.decimal_context.create_decimal_from_float(round(float(odd[2]), 2))     #odd[2] is betOdd2
                                        round(Decimal(odd[2]), 2),     #odd[2] is betOdd2
                                        self.inserting_date_and_time    #appended to know deprecated bets on updating
                                        ))  
      
    def find_matches_with_three_outcomes(self):
        select_bets_with_three_outcomes_in_list_query = f"""SELECT * 
                                                        FROM {self.SQLITE_SCRAPED_BETS_TABLE}
                                                """
        self.sqlite_cursor.execute(select_bets_with_three_outcomes_in_list_query)
        
        records = self.sqlite_cursor.fetchall()

        start_finding_outcomes = time.time()

        for row in records: #for each bet, finds the opposite ones 
            self.find_opposite_bet_three_outcomes(row)
            if len(self.BetMatches) > self.UPLOAD_TO_AZURE_RECORDS_LIMIT:  #sends data to the server every 1000 rows
                self.azure_cursor.executemany(self.SQL_MERGE_THREE_OUTCOMES, self.BetMatches)
                # self.azure_cursor.executemany(f'''
                #  INSERT INTO {self.BET_MATCHES_THREE_OUTCOMES_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                #     ''', self.BetMatches)
                self.BetMatches.clear()

        END = time.time()
        print("TEMPO MATCHING EVENTI A TRE ESITI:", (END-start_finding_outcomes))

        if (self.BetMatches):
            self.azure_cursor.executemany(self.SQL_MERGE_THREE_OUTCOMES, self.BetMatches)
            # self.azure_cursor.executemany(f'''
            #         INSERT INTO {self.BET_MATCHES_THREE_OUTCOMES_TABLE} VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            #     ''', self.BetMatches)
        
        self.azure_cursor.execute(f"""delete from {self.BET_MATCHES_THREE_OUTCOMES_TABLE} where insertingdate < '{self.deleting_date_and_time}' """)

        #create an index on rtp column to speed up ordering
        # start_index = time.time()
        # self.azure_cursor.execute(self.CREATE_INDEX_ON_BET_MATCHES_THREE_OUTCOMES_TABLE)
        # end_index = time.time()
        # print("TEMPO INDICE TRE ESITI:", (end_index-start_index))

        self.BetMatches.clear()
        self.azure_connection.commit()
        # self.sqlite_connection.commit()
        

    def find_opposite_bet_three_outcomes(self, row):
        #query to get the second outcome
        find_opposite_bet_2_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd
                        FROM {self.SQLITE_SCRAPED_BETS_TABLE}
                        WHERE betRadarID = "{row[2]}" and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[0]}" and bookmaker != "{row[0]}"
                        """
        self.sqlite_cursor.execute(find_opposite_bet_2_three_outcomes_query)
        
        opposite_odd_2 = self.sqlite_cursor.fetchall()    #not fetchone() because many results from multiple bookmakers can be obtained (e.g. juve-inter 1 has counter x2 on multiple bookmakers)
        if opposite_odd_2:
            #query to get the third outcome
            find_opposite_bet_3_three_outcomes_query = f"""SELECT bookmaker, bet, betOdd 
                            FROM {self.SQLITE_SCRAPED_BETS_TABLE}
                            WHERE  betRadarID = "{row[2]}" and bet = "{self.CounterBetsThreeOutcomes.get(row[5])[1]}" and bookmaker != "{row[0]}"
                            """ 
            self.sqlite_cursor.execute(find_opposite_bet_3_three_outcomes_query)

            opposite_odd_3 = self.sqlite_cursor.fetchall() 

            for odd2 in opposite_odd_2:    #more than one bookmaker have the same bets
                for odd3 in opposite_odd_3:
                    if odd2[0] != odd3[0]:  #check if the third bookmaker is different from the second one 
                        self.BetMatches.append((self.compute_rtp_three_outcomes(float(row[6]), float(odd2[2]), float(odd3[2])), #appends data to the record list
                                                row[1],    #row[1] is tournament
                                                row[2],    #row[2] is betRadarID
                                                row[3],    #row[3] is event
                                                row[4],    #row[4] is date
                                                row[0],    #row[0] is bookmaker1
                                                re.sub(r'^.*? <> ', '', row[5]),    #row[5] is bet1
                                                # self.decimal_context.create_decimal_from_float(round(float(row[6]), 2)),    #row[6] is betOdd1
                                                round(Decimal(row[6]), 2),    #row[6] is betOdd1
                                                odd2[0],    #odd2[0] is bookmaker2
                                                re.sub(r'^.*? <> ', '', odd2[1]),    #odd2[1] is bet2
                                                # self.decimal_context.create_decimal_from_float(round(float(odd2[2]), 2)),    #odd2[2] is betOdd2
                                                round(Decimal(odd2[2]), 2),    #odd2[2] is betOdd2
                                                odd3[0],    #odd3[0] is bookmaker3
                                                re.sub(r'^.*? <> ', '', odd3[1]),    #odd3[1] is bet3
                                                # self.decimal_context.create_decimal_from_float(round(float(odd3[2]), 2)),    #odd3[2] is betOdd3
                                                round(Decimal(odd3[2]), 2),    #odd3[2] is betOdd3
                                                self.inserting_date_and_time    #appended to know deprecated bets on updating
                                                )) 
                  

    # def compute_rtp_two_outcomes(self,firstOdd, secondOdd):   #computes rtp based on odds
    #     result = ((firstOdd * secondOdd) / (firstOdd + secondOdd)) * 100
    #     return round(result, 2)

    def compute_rtp_two_outcomes(self,q1, q2):   #computes rtp based on odds
        # result = ((q1  *(q2 - 1)) / q2) * 100
        result = (q1  *(1.0 - 1.0/q2)) * 100
        return round(result, 2)
    
    # def compute_rtp_three_outcomes(self,firstOdd, secondOdd, thirdOdd):   #computes rtp based on odds. thirdOdd is because this method can be invoke with three bets
    #     result = (firstOdd * secondOdd * thirdOdd) / (firstOdd * secondOdd + secondOdd * thirdOdd + firstOdd * thirdOdd) * 100
    #     return round(result, 2)

    def compute_rtp_three_outcomes(self,q1, q2, q3):   #computes rtp based on odds. thirdOdd is because this method can be invoke with three bets
        # result = ((q1 * ((q2 - 1)*q3 - q2)) / (q2 * q3)) * 100
        result = (q1 * (1.0 - 1.0/q2 -1.0/q3)) * 100
        return round(result, 2)


# start = time.time()
# comparator = BetComparatorFromAzureDB()
# comparator.find_matches()
# end = time.time()
# print("TEMPO TOTALE: ", (end - start), 'secondi')

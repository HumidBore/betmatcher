import pyodbc 
import time
import timeit
from datetime import datetime
import pandas 
from decimal import Decimal
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'tcp:betmatcher.database.windows.net' 
database = 'betmatcher' 
username = 'azureadmin' 
password = 'xcazU7qpal3.' 
driver= '{ODBC Driver 17 for SQL Server}'


with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
    with conn.cursor() as cursor:
        # print(datetime.now())
        SQL_MERGE_TWO_OUTCOMES =  """
            MERGE INTO [dbo].[bet_matches_two_outcomes] AS results
            USING (VALUES (?,?,?,?,?,?,?,?,?,?,?,?)) AS newval(RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,insertingdate)
                    ON results.betRadarID = newval.betRadarID and
                    results.bookmaker1 = newval.bookmaker1 and
                    results.bet1 = newval.bet1 and
                    results.bookmaker2 = newval.bookmaker2 and
                    results.bet2 = newval.bet2
            WHEN MATCHED THEN
                    UPDATE SET results.betOdd1 = newval.betOdd1, results.betOdd2 = newval.betOdd2, results.insertingdate = newval.insertingdate
            WHEN NOT MATCHED THEN
                    INSERT (RTP,tournament,betRadarID,event,date,bookmaker1,bet1,betOdd1,bookmaker2,bet2,betOdd2,insertingdate) 
                    VALUES (newval.RTP, newval.tournament, newval.betRadarID, newval.event, newval.date, newval.bookmaker1, newval.bet1, newval.betOdd1,newval.bookmaker2, newval.bet2, newval.betOdd2,newval.insertingdate);
            """ 
        row = (round(Decimal("100"), 2),'Clausura', 26587112,'RIVER PLATE URU - PROGRESO','2021-03-29T21:00:00.0000000','SportPesa','1', round(Decimal("3.0"),2), 'AdmiralYes', "X2", round(Decimal("3"),2), "2021-04-05T09:29:29.0000000")
        # cursor.execute("""INSERT INTO [dbo].[bet_matches_two_outcomes] 
        # VALUES (?,?,?,?,?,?,?)
        # """, row)
        # cursor.execute(SQL_MERGE_TWO_OUTCOMES, row)
        inserting_date_and_time = datetime.now()
        cursor.execute(f"""delete from bet_matches_two_outcomes where insertingdate < '{inserting_date_and_time}'""")



        
        # cursor.execute("""SELECT bookmaker, bet, betOdd 
        #     FROM scraping_results
        #     WHERE  betRadarID = 26451110 and bet = 'U/O 2.5 <> UNDER' and bookmaker != 'SportPesa'
        # """)
        # df = pd.read_sql_query(
        #             '''select *
        #                 from [dbo].[scraping_results]''', conn, index_col=['bookmaker','betRadarID'])
                        # , index_col=['bookmaker','betRadarID', 'event', 'date', 'bet']
        
        # result = df.loc[(df['bookmaker'] != 'SportPesa') & 
        #                 (((df['betRadarID'] == 26451110) & df['betRadarID'] != -1) | ((df['event'] == 'FENIX MONTEVIDEO - CERRO LARGO') & df['date'] == '2021-03-21 20:00:00.000')) &
        #                 df['bet'] == 'U/O 2.5 <> UNDER']
        # result = df.loc[(df['bookmaker'] != 'SportPesa') & (df['betRadarID'] == 26451110) & (df['bet'] == 'U/O 2.5 <> UNDER')]
        # result = df.query('bookmaker == "SportPesa"')

        # start = time.time()
        
        # result = df.loc[(df['betRadarID'] == 26451110) & (df['bookmaker'] != 'SportPesa') & (df['bet'] == 'U/O 2.5 <> UNDER')]
        # for i in range(2):
        #     result = df[(df.index.get_level_values('bookmaker') != 'SportPesa') & 
        #         (df.index.get_level_values('betRadarID') == 26451110) &
        #         (df['bet'] == 'U/O 2.5 <> UNDER')
                        # ]
#         code_to_test = """
# df.loc[(df['betRadarID'] == 26451110) & (df['bookmaker'] != 'SportPesa')]
#         """
#         elapsed_time = timeit.timeit(code_to_test, number=100)/100
        # print(elapsed_time)
        # end = time.time()
        # print(result)
        # print(df.columns)
        # print(df.head(20))        
        # print("TEMPO: ", (end - start)/2)
        
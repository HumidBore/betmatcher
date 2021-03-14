# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3 #to use sqlite3
from datetime import datetime
from betmatcher.betTypeAdjuster import BetTypeAdjuster  #to format bets, creating a unique format which allows to match them
import os
import time

class BetmatcherPipeline(object):

    def process_item(self, item, spider):
        return item


#############################################
#TO STORE DATA IN SQLlite3 DATABASE
#############################################  

class SQLlitePipeline(object):
    
    
    PATH = '.\\Databases\\'     #relative path to the db (.\\ stands for betmatcher outer folder)
    DB_NAME = "TESTbetmatcher.db"
    TABLE_NAME = "test_multiple_spiders"
    # TABLE_NAME = "BetMan"

    ROWS_TO_COMMIT = 10000   #number of rows to insert together (more efficient than insert one by one each row)

    def __init__(self):
        self.records = []  # MUST BE CHANGED WHEN MULTIPLE SPIDERS RUN
        

    #I created inside the open spide method because I want to create a db only once in a lifetime of the spider
    def open_spider(self, spider):  #method called (only once) when the spider is opened
        self.start = time.time()
        
        self.adjuster = BetTypeAdjuster(spider.name)    #a BetTypeAdjuster is an object to format bet Types, creating a unique format for all the bookmakers
        
        self.connection = sqlite3.connect(self.PATH + self.DB_NAME)    #connect to the database specified as string (.db is the extension for databases in sqllite3)
        self.cursor = self.connection.cursor()   #create a cursor (1st step to execute queries)
        
        
        self.createTable()


#for each item scraped the process_item is called
    def process_item(self, item, spider):
        adjusted_bet = self.adjuster.replace_bets(item.get('bet'))
        if self.adjuster.is_allowed_bet(adjusted_bet):  #check whether the bet should be appended or not
            self.records.append((spider.name,
                                item.get('tournament'),    #appends data to the record list
                                item.get('betRadarID'),
                                item.get('event'),
                                item.get('date'),
                                self.adjuster.replace_bets(item.get('bet')),
                                self.adjuster.fractional_to_decimal_odd(item.get('betOdd'))))
            
            if (len(self.records) >= self.ROWS_TO_COMMIT):   #every ROWS_TO_COMMIT records the block is inserted in db
                self.commit_block()

        return item


    def close_spider(self, spider): #method called (only once) when the spider has finshed scraping items
        self.last_commit()
        
        end = time.time()
        print("TEMPO TRASCORSO: ", (end - self.start) / 60.0, 'minuti')

 
#invoked in process_item() if it is time to commit the sotred records
    def commit_block(self):
        self.cursor.executemany(f'''
                 INSERT OR REPLACE INTO {self.TABLE_NAME} (bookmaker,tournament,betRadarID,event,date,bet,betOdd) VALUES(?,?,?,?,?,?,?)
             ''', self.records)
        self.records.clear()
        self.connection.commit()

#invoked in close_spider() to commit the remaining records
    def last_commit(self):
        self.cursor.executemany(f'''
                 INSERT OR REPLACE INTO {self.TABLE_NAME} (bookmaker,tournament,betRadarID,event,date,bet,betOdd) VALUES(?,?,?,?,?,?,?)
             ''', self.records)
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        
#Invoked in open_spider() to create a new table to store the records
    def createTable(self):  
        try:
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.TABLE_NAME}(
                    bookmaker TEXT,
                    tournament TEXT,
                    betRadarID INT,
                    event TEXT,
                    date TEXT,
                    bet TEXT,
                    betOdd REAL,

                    PRIMARY KEY(bookmaker,betRadarID,event,bet)
                )
            ''')
            self.connection.commit()    #save chenges (commit)
        except sqlite3.OperationalError:    #checks if the table already exists
            pass



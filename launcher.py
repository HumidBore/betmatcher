
# from betmatcher import betTypeAdjuster
# from betTypeAdjuster import BetTypeAdjuster
import re
import time
from datetime import datetime, timedelta
from datetime import date
import sqlite3
# from betmatcher.spiders import SportPesa
from betmatcher.spiders import SportPesa
from betmatcher.spiders import MerkurWin
from betmatcher.spiders import BetMan
from betmatcher.spiders import AdmiralYes
from betmatcher.spiders import Sport888

from scrapy.crawler import CrawlerProcess

from multiprocessing import Process #needed because when midlleware stops the process to retry the request, the other crawler must not be stopped
import os

start = time.time()

# process = CrawlerProcess()
# process.crawl(SportPesa.SportpesaSpider())
# process.crawl(MerkurWin.MerkurwinSpider())
# process.crawl(BetMan.BetmanSpider())
# process.crawl(AdmiralYes.AdmiralyesSpider())
# process.crawl(Sport888.A888sportSpider())
# process.start()

def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def launchSportPesa():
    info('FUNCTION launchSportPesa')
    crawlerProcessSportPesa = CrawlerProcess()
    crawlerProcessSportPesa.crawl(SportPesa.SportpesaSpider())
    crawlerProcessSportPesa.start()

def launchMerkurWin():
    info('FUNCTION launchMerkurWin')
    crawlerProcessMerkurWin = CrawlerProcess()
    crawlerProcessMerkurWin.crawl(MerkurWin.MerkurwinSpider())
    crawlerProcessMerkurWin.start()

def launchBetMan():
    info('FUNCTION launchBetMan')
    crawlerProcessBetMan = CrawlerProcess()
    crawlerProcessBetMan.crawl(BetMan.BetmanSpider())
    crawlerProcessBetMan.start()

def launchAdmiralYes():
    info('FUNCTION launchAdmiralYes')
    crawlerProcessAdmiralYes = CrawlerProcess()
    crawlerProcessAdmiralYes.crawl(AdmiralYes.AdmiralyesSpider())
    crawlerProcessAdmiralYes.start()

def launchSport888():
    info('FUNCTION launchSport888')
    crawlerProcessSport888 = CrawlerProcess()
    crawlerProcessSport888.crawl(Sport888.A888sportSpider())
    crawlerProcessSport888.start()

def runInParallel(*functions):  #runs functions in parallel, creating a process for each bookmaker
    processes = []
    for function in functions:
        process = Process(target=function)
        process.start()
        processes.append(process)
    for p in processes:
        p.join()

if __name__ == '__main__':
    PATH = '.\\Databases\\'     #relative path to the db (.\\ stands for betmatcher outer folder)
    DB_NAME = "TESTbetmatcher.db"
    info('main line')
    # connection = sqlite3.connect(PATH + DB_NAME)    #connect to the database specified as string (.db is the extension for databases in sqllite3)
    # cursor = connection.cursor()
    # print("connected")
    # cursor.execute('delete from test_multiple_spiders')
    # connection.commit()
    # cursor.close()
    # connection.close()
    # print("deleted and closed")
    runInParallel(launchMerkurWin,
                launchSport888,
                launchBetMan,
                launchSportPesa,
                launchAdmiralYes
                )

# end = time.time()
# print("TEMPO TOTALE: ", (end - start) / 60.0, 'minuti')



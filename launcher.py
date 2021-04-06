
import re
import time
from datetime import datetime, timedelta
from datetime import date
import sqlite3

from betmatcher.spiders import SportPesa
from betmatcher.spiders import MerkurWin
from betmatcher.spiders import BetMan
from betmatcher.spiders import AdmiralYes
from betmatcher.spiders import Sport888

from bet_comparator import BetComparatorFromAzureDB

from scrapy.crawler import CrawlerProcess

from multiprocessing import Process #needed because when midlleware stops the process to retry the request, the other crawler must not be stopped
import os

# start = time()


def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

def launchComparator():
    info('Inizio comparatore')
    comparator = BetComparatorFromAzureDB()
    comparator.find_matches()

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
    start = time.time()
    for function in functions:
        process = Process(target=function)
        process.start()
        processes.append(process)
    for p in processes:
        p.join()
    print("Terminato lo scraper (dentro run in parallel)")
    launchComparator()
    print("Print da run in parallel finito il comparator")
    print("TEMPO TOTALE:", time.time()-start)

if __name__ == '__main__':
    
    info('main line')
    runInParallel(launchMerkurWin,
                launchSport888,
                launchBetMan,
                launchSportPesa,
                launchAdmiralYes
                )
    # print("Terminato lo scraping (dentro name = main)")
    
   
    # runInParallel(launchSportPesa)
    

# print("Print dall'esterno")
# end = time.time()
# print("TEMPO TOTALE: ", (end - start) / 60.0, 'minuti')



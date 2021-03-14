-- SQLite
select distinct  bet
from sportpesa_merkurwin
where bookmaker == 'SportPesa' 
order by bet;

select distinct  bet
from Sport888
where bet not like '%vince almeno un tempo%' and bet not like '%vince entrambi i tempi%'
            and bet not like '%segna un gol in entrambi i tempi%'
            and bet not like '%segna un gol su calcio di rigore%'
            and bet not like '%Handicap%'
            and bet not like '%Angoli%' 
            and bet not like '%Calcio di rigore assegnato%' 
            and bet not like '%Cartellini totali%' 
            and bet not like '%Gol totali - %' 
            and bet not like '%Marcatore%' 
            and bet not like '%turno%' 
            and bet not like '%Posizione finale%' 
            
order by bet;

select *
from test_multiple_spiders
where event == 'Parma Calcio - Inter'

EXPLAIN QUERY PLAN
select *
from bet_matches
WHERE RTP > 95
order by RTP desc


delete  from BetMan
delete  from test_multiple_spiders

select distinct bookmaker from test_multiple_spiders 

select distinct  event
from test_multiple_spiders 


select distinct  bookmaker
from test_multiple_spiders 


drop table AdmiralYes;
drop table bet_matches;
drop table copy_test_multiple_spiders;

drop table copy_test_multiple_spiders;

drop table test_multiple_spiders;

drop table merkurwin_football_2021_02_23_09_11_52;
drop table merkurwin_football_2021_02_23_09_38_28

Select * from SQLite_master

SELECT distinct t.bookmaker, t.event 
from test_multiple_spiders t join Sport888 s on t.event == s.event 
where t.betRadarID not NULL
order by t.event;

SELECT distinct bet from Sport888 order by bet;

SELECT distinct bookmaker, event from test_multiple_spiders where betRadarID not NULL order by event;



CREATE TABLE IF NOT EXISTS Sport888 (
                    bookmaker TEXT,
                    tournament TEXT,
                    betRadarID INT,
                    event TEXT,
                    date TEXT,
                    bet TEXT,
                    betOdd REAL,

                    PRIMARY KEY(bookmaker,betRadarID,bet)
                )


SELECT * 
from test_multiple_spiders outerTable
where bet in ("U/O 0.5 <> OVER", "U/O 1.5 <> OVER") and EXISTS (select * 
                                                        from test_multiple_spiders innerTable
                                                        where outerTable.betRadarID == innerTable.betRadarID
                                                            and outerTable.bet == innerTable.bet
                                                            and outerTable.bookmaker != innerTable.bookmaker)

CREATE INDEX rtp_index ON bet_matches(RTP)

drop index rtp_index

CREATE INDEX bet_index ON test_multiple_spiders(bet)

CREATE INDEX betRadarID_Bet_index ON test_multiple_spiders(betRadarID,bet)

CREATE INDEX betRadarID_Bet_bookmaker_index ON test_multiple_spiders(betRadarID,bet,bookmaker)

select * 
from bet_matches
order by RTP desc

drop table bet_matches

drop index betRadarID_Bet_index

drop index betRadarID_Bet_bookmaker_index

drop index betRadarID_index

drop index bet_index

Select * from SQLite_master

CREATE TABLE IF NOT EXISTS copy_table AS 
    SELECT * FROM test_multiple_spiders
    
drop table copy_table
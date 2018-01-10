import requests
import json
import os.path
import getpass
from datetime import datetime
from sqltools import *
from common import *
from decimal import Decimal
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

def updatePrices():
  updateBTC()
  updateOMNISP()
  updateFEES()
  dbCommit()

def updateFEES():
  #Get bitgo fees
  faster=[]
  fast=[]
  normal=[]
  #Get BitGo Fee's
  try:
    source='https://www.bitgo.com/api/v1/tx/fee'
    r= requests.get( source, timeout=15 )
    feelist=r.json()
    q=[]
    for x in feelist['feeByBlockTarget']:
      if feelist['feeByBlockTarget'][x] not in q:
        q.append(feelist['feeByBlockTarget'][x])

    q.sort(reverse=True)
    faster.append(q[0])
    fast.append(q[1])
    normal.append(q[2])
  except Exception as e:
    #error or timeout, skip for now
    printdebug(("Error getting BitGo fees",e),3)
    pass
  #Get Blockcypher Fee's
  try:
    source='http://api.blockcypher.com/v1/btc/main'
    r= requests.get( source, timeout=15 )
    feelist=r.json()
    faster.append(feelist['high_fee_per_kb'])
    fast.append(feelist['medium_fee_per_kb'])
    normal.append(feelist['low_fee_per_kb'])
  except Exception as e:
    #error or timeout, skip for now
    printdebug(("Error getting Blockcypher fees",e),3)
    pass
  #Get Bitcoinfees21 Fee's
  try:
    #source='https://bitcoinfees.21.co/api/v1/fees/list'
    source='https://bitcoinfees.21.co/api/v1/fees/recommended'
    r= requests.get( source, timeout=15 )
    feelist=r.json()
    #for x in feelist['fees']:
    #  if x['maxDelay']>0 and x['maxDelay']<=7:
    #    fr=int(((x['minFee']+x['maxFee'])/2)*1000)
    #  if x['maxDelay']>7 and x['maxDelay']<=20:
    #    f=int(((x['minFee']+x['maxFee'])/2)*1000)
    #  if x['maxDelay']>20 and x['maxDelay']<=40:
    #    n=int(((x['minFee']+x['maxFee'])/2)*1000)
    fr=int(feelist['fastestFee']*1000)
    f=int(feelist['halfHourFee']*1000)
    n=int(feelist['hourFee']*1000)
    faster.append(fr)
    fast.append(f)
    normal.append(n)
  except Exception as e:
    #error or timeout, skip for now
    printdebug(("Error getting bitcoinfees21 fees",e),3)
    pass
  fr=int(sum(faster)/len(faster))
  ff=int(sum(fast)/len(fast))
  nf=int(sum(normal)/len(normal))
  data=json.dumps({'faster':fr,'fast':ff,'normal':nf})
  dbExecute("with upsert as "
              "(update settings set value=%s, updated_at=DEFAULT where key='feeEstimates' returning *) "
              "insert into settings (key, value) select 'feeEstimates',%s "
              "where not exists (select * from upsert)",
              (data, data))



def fiat2propertyid(abv):
  ROWS=dbSelect("select propertyid from smartproperties where protocol='Fiat' and propertyname=%s",[abv.upper()])
  if len(ROWS) == 0:
    return -1
  else:
    return ROWS[0][0]


def getSource(sp):
  try:
    #convert={1:"https://masterxchange.com/api/trades.php",
    #         1:"https://poloniex.com/public?command=returnTradeHistory&currencyPair=BTC_OMNI",
    #         3:"https://masterxchange.com/api/v2/trades.php?currency=maid"
    #         3:"https://poloniex.com/public?command=returnTradeHistory&currencyPair=BTC_MAID",
    #         39:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-AMP&count=100",
    #         56:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-SAFEX&count=100",
    #         58:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-AGRS&count=100",
    #         59:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-PDC&count=100",             
    #         89:"https://api.livecoin.net/exchange/last_trades?currencyPair=DIBC/BTC",
    #        }
    convert={1:{"id":"OMNI","source":"coinmarketcap"},
             3:{"id":"MAID","source":"coinmarketcap"},
             31:{"id":"USDt","source":"fixed","value":1,"base":0},
             39:{"id":"AMP","source":"coinmarketcap"},
             41:{"id":"EURt","source":"fixed","value":1,"base":2},
             56:{"id":"SAFEX","source":"coinmarketcap"},
             58:{"id":"AGRS","source":"coinmarketcap"},
             59:{"id":"PDC","source":"coinmarketcap"},
             66:{"id":"GARY","source":"coinmarketcap"},
             89:{"id":"DIBC","source":"coinmarketcap"},
             90:"https://market.bitsquare.io/api/trades?market=sfsc_btc",
             149:{"id":"ALT","source":"coinmarketcap"}
            }
    return convert[sp]
  except KeyError:
    return None

def getfixedprice(desiredvalue, base):
  #base id, currency
  #  0 | USD       #  1 | CAD       #  2 | EUR       #  3 | AUD
  #  4 | IDR       #  5 | ILS       #  6 | GBP       #  7 | RON
  #  8 | SEK       #  9 | SGD       # 10 | HKD       # 11 | CHF
  # 12 | CNY       # 13 | TRY       # 14 | NZD       # 15 | NOK
  # 16 | RUB       # 17 | MXN       # 18 | BRL       # 19 | PLN       
  # 20 | ZAR       # 21 | JPY

  ROWS=dbSelect("select rate1for2 from exchangerates where protocol1='Fiat' and propertyid1=%s and protocol2='Bitcoin' and propertyid2=0 "
                "order by asof desc limit 1",[int(base)])
  if len(ROWS)>0:
    return desiredvalue / ROWS[0][0]
  else:
    return 0

def upsertRate(protocol1, propertyid1, protocol2, propertyid2, rate, source, timestamp=None):

  if propertyid1 < 0 or propertyid2 < 0:
    printdebug(("Error, can't insert invalid propertyids", propertyid1, "for", propertyid2), 4)
    return

  if timestamp==None:
    # if we have a record with the same exchangerate / source just update timestamp, otherwise insert new record
    dbExecute("with upsert as "
                "(update exchangerates set asof=DEFAULT where protocol1=%s and propertyid1=%s and "
                " protocol2=%s and propertyid2=%s and rate1for2::numeric=%s and source=%s  returning *) "
              "insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source) select %s,%s,%s,%s,%s,%s "
              "where not exists (select * from upsert)",
              (protocol1, propertyid1, protocol2, propertyid2, rate, source, protocol1, propertyid1, protocol2, propertyid2, rate, source))
  else:
    # if we have a record with the same exchangerate / source just update timestamp, otherwise insert new record
    dbExecute("with upsert as "
                "(update exchangerates set asof=%s where protocol1=%s and propertyid1=%s and "
                " protocol2=%s and propertyid2=%s and rate1for2::numeric=%s and source=%s  returning *) "
              "insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source, asof) select %s,%s,%s,%s,%s,%s,%s "
              "where not exists (select * from upsert)",
              (timestamp, protocol1, propertyid1, protocol2, propertyid2, rate, source, protocol1, propertyid1, protocol2, propertyid2, rate, source, timestamp))

def updateBTC():
    try:
      source='https://apiv2.bitcoinaverage.com/constants/exchangerates/global'
      r= requests.get( source, timeout=15 )
      curlist=r.json()
      #timestamp=curlist.pop('timestamp')
      if 'ignored_exchanges' in curlist:
        curlist.pop('ignored_exchanges')
      btc=curlist['rates']['BTC']['rate']
      timestamp=curlist['time']
      new=[]
      for abv in curlist['rates']:
        value = Decimal(curlist['rates'][abv]['rate']) / Decimal(btc)
        value = float(int(Decimal(value) * Decimal(1e2)) / Decimal(1e2))
        #get our fiat property id using internal conversion schema
        fpid=fiat2propertyid(abv)
        if fpid == -1:
          new.append(abv)
        else:
          upsertRate('Fiat', fpid, 'Bitcoin', 0, value, source, timestamp)
      if len(new) > 0:
        printdebug(("New Symbols not in db",new),5)
    except requests.exceptions.RequestException as e:
      #error or timeout, skip for now
      printdebug(("Error updating BTC Price",e),3)
      pass


def formatData(sp, source):
  trades=[]
  r = requests.get( source, timeout=15 )

  try:
    trades=r.json()
  except ValueError:
    trades=eval(r.content)

  try:
    if 'coinmarketcap' in source:
      tmap={}
      for x in trades:
        tmap[x['symbol']]=x
      trades=tmap 
    else:
      if sp in [39,58,59]:
        trades=trades['result']
        for trade in trades:
          trade['rate']=trade['Price']
          trade['amount']=trade['Quantity']  
      
      if sp in [89]:
        for trade in trades:
          trade['rate']=trade['price']
          trade['amount']=trade['quantity']
      
      if sp in [90]:
        for trade in trades:
          trade['rate']=trade['price']

  except TypeError:
    trades=[]

  return trades

def updateOMNISP():
  try:
    #get list of smart properties we know about
    ROWS=dbSelect("select propertyid from smartproperties where propertyid >0 and Protocol='Omni' order by propertyid")
    #get Coinmarket Cap data
    cmcSource="https://api.coinmarketcap.com/v1/ticker/?limit=0"
    cmcData=formatData(0, cmcSource)

    for x in ROWS:
      sp=x[0]  
      src=getSource(sp)
      if src != None:
        try:
          if 'source' in src and src['source'] == 'coinmarketcap':
            value=Decimal(cmcData[src['id']]['price_btc'])
            source=str(cmcSource)+str("&symbol=")+str(src['id'])
          elif 'source' in src and src['source'] == 'fixed':
            #Fix sp value
            source='Fixed'
            value=getfixedprice(src['value'],src['base'])
          else:
            source=src
            trades=formatData(sp, source)
            volume = 0;
            sum = 0;
            value = 0;
            for trade in trades:
              volume += float( trade['amount'] )
              sum += float( trade['amount'] ) * float(trade['rate'] )
            if volume != 0:
              value=(sum / volume)
        except Exception as e:
          printdebug(("OMNISP Error processing:",e,sp,src),3)
          pass
      else:
        #no Known source for a valuation, set to 0
        value=0
        source='Local'

      upsertRate('Bitcoin', 0, 'Omni', sp, value, source)

  except requests.exceptions.RequestException as e:
    #error or timeout, skip for now
    printdebug(("Error updating OMNISP Prices",e),3)
    pass


def main():
  USER=getpass.getuser()
  lockFile='/tmp/updatePrices.lock'+str(USER)
  now=datetime.now()

  if os.path.isfile(lockFile):
    #open the lock file to read pid and timestamp
    file=open(lockFile,'r')
    pid=file.readline().replace("\n", "")
    timestamp=file.readline()
    file.close()
    #check if the pid is still running
    if os.path.exists("/proc/"+str(pid)):
      print "Exiting: updatePrices already running with pid:", pid, "  Last update started at ", timestamp
    else:
      print "Stale updatePrices found, no running pid:", pid, " Process last started at: ", timestamp
      print "Removing lock file and waiting for restart"
      os.remove(lockFile)
    #exit program and wait for next run
    exit(1)
  else:
    #start/create our lock file
    file = open(lockFile, "w")
    file.write(str(os.getpid()))
    file.write(str(now))
    file.close()

    #set our debug level, all outputs will be controlled by this
    setdebug(9)

    try:
      updatePrices()
    except Exception as e:
      #Catch any issues and stop processing. Try to undo any incomplete changes
      print "updatePrices: Problem with ", e
      if dbRollback():
        print "Database rolledback"
      else:
        print "Problem rolling database back"
      os.remove(lockFile)
      exit(1)

  #remove the lock file and let ourself finish
  os.remove(lockFile)




if __name__ == "__main__":main() ## with if

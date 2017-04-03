import requests
import json
import os.path
import getpass
from datetime import datetime
from sqltools import *
from common import *
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
    faster.append(feelist['feeByBlockTarget']['2'])
    fast.append(feelist['feeByBlockTarget']['5'])
    normal.append(feelist['feeByBlockTarget']['6'])
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
    #         3:"https://masterxchange.com/api/v2/trades.php?currency=maid"
    #        }
    convert={1:"https://poloniex.com/public?command=returnTradeHistory&currencyPair=BTC_OMNI",
             3:"https://poloniex.com/public?command=returnTradeHistory&currencyPair=BTC_MAID",
             39:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-AMP&count=100",
             56:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-SEC&count=100",
             58:"https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-AGRS&count=100"
            }
    return convert[sp]
  except KeyError:
    return None

def getfixedprice(desiredvalue):
        ROWS=dbSelect("select rate1for2 from exchangerates where protocol1='Fiat' and propertyid1=0 and protocol2='Bitcoin' and propertyid2=0 "
                      "order by asof desc limit 1")
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
                " protocol2=%s and propertyid2=%s and rate1for2=%s and source=%s  returning *) "
              "insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source) select %s,%s,%s,%s,%s,%s "
              "where not exists (select * from upsert)",
              (protocol1, propertyid1, protocol2, propertyid2, rate, source, protocol1, propertyid1, protocol2, propertyid2, rate, source))
  else:
    # if we have a record with the same exchangerate / source just update timestamp, otherwise insert new record
    dbExecute("with upsert as "
                "(update exchangerates set asof=%s where protocol1=%s and propertyid1=%s and "
                " protocol2=%s and propertyid2=%s and rate1for2=%s and source=%s  returning *) "
              "insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source, asof) select %s,%s,%s,%s,%s,%s,%s "
              "where not exists (select * from upsert)",
              (timestamp, protocol1, propertyid1, protocol2, propertyid2, rate, source, protocol1, propertyid1, protocol2, propertyid2, rate, source, timestamp))

def updateBTC():
    try:
      source='https://api.bitcoinaverage.com/all'
      r= requests.get( source, timeout=15 )
      curlist=r.json()
      #timestamp=curlist.pop('timestamp')
      if 'ignored_exchanges' in curlist:
        curlist.pop('ignored_exchanges')
      for abv in curlist:
        value=curlist[abv]['averages']['last']
        timestamp=curlist[abv]['averages']['timestamp']
        #get our fiat property id using internal conversion schema  
        fpid=fiat2propertyid(abv)
        if fpid == -1:
           printdebug(("Currency Symbol",abv,"not in db. New currency?"),5)
        else:
          upsertRate('Fiat', fpid, 'Bitcoin', 0, value, source, timestamp)

      #currencies removed from bitcoinaverage
      for abv in ['CHF', 'AUD', 'NOK', 'HKD', 'RON']:
        source2='http://download.finance.yahoo.com/d/quotes.csv?s=USD'+abv+'=X&f=snl1d1t1ab'
        r2= requests.get( source2, timeout=15 )
        data=r2.text.split(',')
        value=float(data[2])*curlist['USD']['averages']['last']
        timestamp=str(data[3])+" "+str(data[4])
        #get our fiat property id using internal conversion schema  
        fpid=fiat2propertyid(abv)
        if fpid == -1:
           printdebug(("Currency Symbol",abv,"not in db. New currency?"),5)
        else:
          upsertRate('Fiat', fpid, 'Bitcoin', 0, value, source2, timestamp)

    except requests.exceptions.RequestException as e:
      #error or timeout, skip for now
      printdebug(("Error updating BTC Price",e),3)
      pass

def formatData(sp, source):
  trades=[]
  source=getSource(sp)
  r = requests.get( source, timeout=15 )

  try:
    trades=r.json()
  except ValueError:
    trades=eval(r.content)

  if sp in [39,56,58]:
    trades=trades['result']
    for trade in trades:
      trade['rate']=trade['Price']
      trade['amount']=trade['Quantity']  


  return trades

def updateOMNISP():
  try:
    #get list of smart properties we know about
    ROWS=dbSelect("select propertyid from smartproperties where propertyid >0 and Protocol='Omni' order by propertyid")
    for x in ROWS:

      sp=x[0]  
      source=getSource(sp)
      if source != None:
        #r = requests.get( source, timeout=15 )
        trades=formatData(sp, source)
        volume = 0;
        sum = 0;
        for trade in trades:
          volume += float( trade['amount'] )
          sum += float( trade['amount'] ) * float(trade['rate'] )

        #try:
        #  for trade in r.json():
        #    volume += float( trade['amount'] )
        #    sum += float( trade['amount'] ) * float(trade['rate'] )
        #except ValueError:
        #  for trade in eval(r.content):
        #    volume += float( trade['amount'] )
        #    sum += float( trade['amount'] ) * float(trade['rate'] )
  
        #BTC is calculated in satashis in getvalue, so adjust our value here to compensate
  
        #value="{:.8f}".format( sum / volume)
        value=(sum / volume)
      elif sp == 31:
        #Temp set sp value to ~$10
        source='Fixed'
        value=getfixedprice(1)
      elif sp == 34:
        #Temp set sp value to ~$10
        source='Fixed'
        value=getfixedprice(10)
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

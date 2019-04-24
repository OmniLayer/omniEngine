import requests
import json
import os.path
import getpass
from datetime import datetime
from sqltools import *
from common import *
from decimal import Decimal
from config import *
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

def updatePrices():
  updateBTC()
  updateOMNISP()
  dbCommit()

def fiat2propertyid(abv):
  ROWS=dbSelect("select propertyid from smartproperties where protocol='Fiat' and propertyname=%s",[abv.upper()])
  if len(ROWS) == 0:
    return -1
  else:
    return ROWS[0][0]


def getSource(sp):
  try:
    convert={0:{"cmcid":"1","name":"BTC","source":"coinmarketcap"},
             1:{"cmcid":"83","name":"OMNI","source":"coinmarketcap"},
             3:{"cmcid":"291","name":"MAID","source":"coinmarketcap"},
             31:{"cmcid":"825","id":"USDt","name":"Tether USD","source":"fixed","value":1,"base":0},
             39:{"cmcid":"1125","name":"AMP","source":"coinmarketcap"},
             41:{"id":"EURt","name":"Tether EUR","source":"fixed","value":1,"base":2},
             56:{"cmcid":"1172","name":"SAFEX","source":"coinmarketcap"},
             58:{"cmcid":"1037","id":"AGRS","name":"IDNO Agoras","source":"coinmarketcap"},
             #59:{"id":"PDC","source":"coinmarketcap"},
             66:{"cmcid":"1352","name":"GARY","source":"coinmarketcap"},
             #89:{"id":"DIBC","source":"coinmarketcap"},
             90:"https://market.bitsquare.io/api/trades?market=sfsc_btc",
             149:{"cmcid":"1642","name":"ALT","source":"coinmarketcap"},
             701:{"cmcid":"3850","id":"OTO","name":"OTOCash","source":"coinmarketcap"}
            }
    if sp == 'cmcids':
      q=[]
      for key in convert.keys():
        try:
          q.append(convert[key]['cmcid'])
        except:
          pass
      q = ','.join(map(str, q))
      return q
    else:
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
    ROWS=dbSelect("select rate1for2, asof from exchangerates where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (protocol1, propertyid1, protocol2, propertyid2))
    if len(ROWS)>0:
      dbrate=ROWS[0][0]
      dbtime=ROWS[0][1]
      if dbrate==rate:
        dbExecute("update exchangerates set asof=DEFAULT where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (protocol1, propertyid1, protocol2, propertyid2))
      else:
        dbExecute("update exchangerates set rate1for2=%s, asof=DEFAULT where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (rate, protocol1, propertyid1, protocol2, propertyid2))
    else:
      dbExecute("insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source) select %s,%s,%s,%s,%s,%s",
              (protocol1, propertyid1, protocol2, propertyid2, rate, source))
  else:
    # if we have a record with the same exchangerate / source just update timestamp, otherwise insert new record
    ROWS=dbSelect("select rate1for2, asof from exchangerates where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (protocol1, propertyid1, protocol2, propertyid2))
    if len(ROWS)>0:
      dbrate=ROWS[0][0]
      dbtime=ROWS[0][1]
      if dbrate==rate:
        dbExecute("update exchangerates set asof=%s where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (timestamp, protocol1, propertyid1, protocol2, propertyid2))
      else:
        dbExecute("update exchangerates set rate1for2=%s, asof=%s where protocol1=%s and propertyid1=%s and protocol2=%s and propertyid2=%s",
                  (rate, timestamp, protocol1, propertyid1, protocol2, propertyid2))
    else:
      dbExecute("insert into exchangerates (protocol1, propertyid1, protocol2, propertyid2, rate1for2, source, asof) select %s,%s,%s,%s,%s,%s,%s",
              (protocol1, propertyid1, protocol2, propertyid2, rate, source, timestamp))

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
  if 'coinmarketcap' in source:
    headers = {'X-CMC_PRO_API_KEY': CMCKEY}
    payload = { 'id' : getSource('cmcids') }
    r = requests.get( source, headers=headers, params=payload, timeout=15 )
  else:
    r = requests.get( source, timeout=15 )

  try:
    trades=r.json()
  except ValueError:
    trades=eval(r.content)

  try:
    if 'coinmarketcap' in source:
      #tmap={}
      #for x in trades:
      #  tmap[x['symbol']]=x
      #trades=tmap
      trades=trades['data']
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
    #cmcSource="https://api.coinmarketcap.com/v1/ticker/?limit=0"
    cmcSource="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    cmcData=formatData(0, cmcSource)

    for x in ROWS:
      sp=x[0]  
      src=getSource(sp)
      if src != None:
        try:
          if 'source' in src and src['source'] == 'coinmarketcap':
            value_usd=Decimal(cmcData[src['cmcid']]['quote']['USD']['price'])
            btc_usd = Decimal(cmcData['1']['quote']['USD']['price'])
            source=str(cmcSource)+str("?id=")+str(src['cmcid'])
            value = value_usd / btc_usd
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

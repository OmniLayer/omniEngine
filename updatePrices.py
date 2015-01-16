import requests
import json
import os.path
from datetime import datetime
from sqltools import *
from common import *


def updatePrices():
  updateBTC()
  updateMSCSP()
  dbCommit()

def fiat2propertyid(abv):
  ROWS=dbSelect("select propertyid from smartproperties where protocol='Fiat' and propertyname=%s",[abv.upper()])
  if len(ROWS) == 0:
    return -1
  else:
    return ROWS[0][0]


def getSource(sp):
  try:
    convert={1:"https://masterxchange.com/api/trades.php",
             3:"https://masterxchange.com/api/v2/trades.php?currency=maid",
            }
    return convert[sp]
  except KeyError:
    return None

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
      timestamp=curlist.pop('timestamp')
      curlist.pop('ignored_exchanges')
      for abv in curlist:
        value=curlist[abv]['averages']['last']
        #get our fiat property id using internal conversion schema  
        fpid=fiat2propertyid(abv)
        upsertRate('Fiat', fpid, 'Bitcoin', 0, value, source, timestamp)

    except requests.exceptions.RequestException, e:
      #error or timeout, skip for now
      printdebug(("Error updating BTC Price",e),3)
      pass


def updateMSCSP():
  try:
    #get list of smart properties we know about
    ROWS=dbSelect("select propertyid from smartproperties where propertyid >0 and Protocol='Mastercoin' order by propertyid")
    for x in ROWS:

      sp=x[0]  
      source=getSource(sp)
      if source != None:
        r = requests.get( source, timeout=15 )
        volume = 0;
        sum = 0;

        for trade in r.json():
            volume += float( trade['amount'] )
            sum += float( trade['amount'] ) * float(trade['price'] )

        #BTC is calculated in satashis in getvalue, so adjust our value here to compensate
  
        #value="{:.8f}".format( sum / volume)
        value=(sum / volume)
      elif sp == 34:
        #Temp set sp value to ~$10
        ROWS=dbSelect("select rate1for2 from exchangerates where protocol1='Fiat' and propertyid1=0 and protocol2='Bitcoin' and propertyid2=0 "
                      "order by asof desc limit 1")
        source='Fixed'
        if len(ROWS)>0:
          value=10 / ROWS[0][0]
        else:
          value=0
      else:
        #no Known source for a valuation, set to 0
        value=0
        source='Local'

      upsertRate('Bitcoin', 0, 'Mastercoin', sp, value, source)

  except requests.exceptions.RequestException:
    #error or timeout, skip for now
    printdebug(("Error updating MSCSP Prices",e),3)
    pass


def main():
  lockFile='/tmp/updatePrices.lock'
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
    except Exception, e:
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

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
  #try:
  #  source='http://api.blockcypher.com/v1/btc/main'
  #  r= requests.get( source, timeout=15 )
  #  feelist=r.json()
  #  faster.append(feelist['high_fee_per_kb'])
  #  fast.append(feelist['medium_fee_per_kb'])
  #  normal.append(feelist['low_fee_per_kb'])
  #except Exception as e:
  #  #error or timeout, skip for now
  #  printdebug(("Error getting Blockcypher fees",e),3)
  #  pass
  #Get Bitcoinfees21 Fee's
  try:
    source='https://bitcoinfees.earn.com/api/v1/fees/recommended'
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

def main():
  USER=getpass.getuser()
  lockFile='/tmp/updateFees.lock'+str(USER)
  now=datetime.now()

  if os.path.isfile(lockFile):
    #open the lock file to read pid and timestamp
    file=open(lockFile,'r')
    pid=file.readline().replace("\n", "")
    timestamp=file.readline()
    file.close()
    #check if the pid is still running
    if os.path.exists("/proc/"+str(pid)):
      print "Exiting: updateFees already running with pid:", pid, "  Last update started at ", timestamp
    else:
      print "Stale updateFees found, no running pid:", pid, " Process last started at: ", timestamp
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
      print "updateFees: Problem with ", e
      if dbRollback():
        print "Database rolledback"
      else:
        print "Problem rolling database back"
      os.remove(lockFile)
      exit(1)

  #remove the lock file and let ourself finish
  os.remove(lockFile)




if __name__ == "__main__":main() ## with if

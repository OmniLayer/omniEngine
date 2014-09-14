from sqltools import *
from rpcclient import *
from sql import *
import json

dbInit()
rows = dbSelect("select txdbserialnum from transactions t where t.txtype=50 or t.txtype=51 or t.txtype=54");
for each in rows:
  txdbserialnum = each[0]
  rawtx={}
  rawtx['result'] = dbSelect("select txdata from txjson where txdbserialnum=" + str(txdbserialnum))[0][0]
  print rawtx
  insertProperty(rawtx, 'Mastercoin', 2147483737)
  #statement = "insert into txjson (txdbserialnum, txdata, protocol) values (" + str(each[2]) + ", \'" + json.dumps(gettransaction_MP(each[0])['result']) + "\', \'" + each[1] + "\')"
  #print statement
  #dbExecute(statement);
  dbCommit();

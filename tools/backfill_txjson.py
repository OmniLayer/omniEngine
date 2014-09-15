from sqltools import *
from rpcclient import *
import json

dbInit()
rows = dbSelect("select * from transactions where txdbserialnum < 46631497;");
for each in rows:
  #print each , gettransaction_MP(each[0])['result'] 
  statement = "insert into txjson (txdbserialnum, txdata, protocol) values (" + str(each[2]) + ", \'" + json.dumps(gettransaction_MP(each[0])['result']) + "\', \'" + each[1] + "\')"
  print statement
  dbExecute(statement);
  dbCommit();

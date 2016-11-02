from sql import *

x=dbSelect("select txhash,txdbserialnum from transactions where txtype>24 and txtype<30 and txstate='valid' order by txdbserialnum")
for y in x:
  rawtx=gettransaction_MP(y[0])
  TxDBSerialNum=y[1]
  print "running, ",TxDBSerialNum
  if rawtx['result']['type_int']==25:
    PropertyIdForSale=rawtx['result']['propertyidforsale']
    PropertyIdDesired=rawtx['result']['propertyiddesired']
    updatemarkets(PropertyIdForSale,PropertyIdDesired,TxDBSerialNum, rawtx)
  else:
    rawtrade=gettrade(y[0])
    for match in rawtrade['result']['cancelledtransactions']:
            #update markets table
            oldtx=gettransaction_MP(match['txid'])
            PropertyIdForSale=oldtx['result']['propertyidforsale']
            PropertyIdDesired=oldtx['result']['propertyiddesired']
            updatemarkets(PropertyIdForSale,PropertyIdDesired,TxDBSerialNum, rawtx)
  

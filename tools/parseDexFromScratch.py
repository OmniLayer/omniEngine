from rpcclient import *
from sql import *

dbInit()

txs=dbSelect("select txblocknumber,txdbserialnum,txtype from transactions where txtype =20 or txtype=22 or txtype=-22 order by txdbserialnum")

Protocol="Mastercoin"

for x in txs:
  Block=x[0]
  TxDBSerialNum=x[1]
  type=x[2]
  print "Processing", TxDBSerialNum

  rawtx={}
  rawtx['result']=json.loads(dbSelect("select txdata from txjson where txdbserialnum=%s", [TxDBSerialNum])[0][0])


  expireAccepts(Block)

  if type == 20:
    if rawtx['result']['valid']:
      updatedex(rawtx, TxDBSerialNum)

  elif type == 22:
    offerAccept(rawtx, TxDBSerialNum, Block)

  elif type == -22:
    Sender=rawtx['result']['sendingaddress']
    for payment in rawtx['result']['purchases']:
      Receiver = payment['referenceaddress']
      PropertyIDBought = payment['propertyid']
      Valid=payment['valid']

      if Valid:
        if getdivisible_MP(PropertyIDBought):
         AmountBought=int(decimal.Decimal(payment['amountbought'])*decimal.Decimal(1e8))
        else:
          AmountBought=int(payment['amountbought'])
        updateAccept(Sender, Receiver, AmountBought, PropertyIDBought, TxDBSerialNum)

  dbCommit()

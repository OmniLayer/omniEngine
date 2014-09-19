from sql import *

txtofix=dbSelect("select atx.txdbserialnum from addressesintxs atx inner join transactions tx on atx.txdbserialnum=tx.txdbserialnum where tx.txtype=50 and tx.txstate='valid'")
#txtofix=dbSelect("select atx.txdbserialnum from addressesintxs atx inner join transactions tx on atx.txdbserialnum=tx.txdbserialnum where tx.txtype=3 and tx.txstate='valid'")
Protocol="Mastercoin"

for tx in txtofix:

      TxDBSerialNum=tx[0]
      rawtx={}
      rawtx['result']=json.loads(dbSelect("select txdata from txjson where txdbserialnum=%s", [TxDBSerialNum])[0][0])

      Address= rawtx['result']['sendingaddress']

      #Now start updating the crowdsale propertyid balance info
      PropertyID = rawtx['result']['propertyid']

      if getdivisible_MP(PropertyID):
        BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
      else:
        BalanceAvailableCreditDebit = int(rawtx['result']['amount'])

      #for updating 'sending' tx i.e. txtype=3
      #BalanceAvailableCreditDebit=BalanceAvailableCreditDebit*-1

      dbExecute("update addressesintxs set BalanceAvailableCreditDebit=%s where Address=%s and propertyid=%s and txdbserialnum=%s and protocol=%s",
                (BalanceAvailableCreditDebit, Address, PropertyID, TxDBSerialNum, Protocol) )


      dbCommit()


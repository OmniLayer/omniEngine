from sql import *

dbInit()

txtofix=dbSelect("select txtype, txhash, txdbserialnum from transactions where txtype=-51 or txtype=51 or txtype=53")
Protocol="Mastercoin"

for tx in txtofix:

      type=tx[0]
      TxHash=tx[1]
      TxDBSerialNum=tx[2]
      rawtx={}
      rawtx['result']=json.loads(dbSelect("select txdata from txjson where txdbserialnum=%s", [TxDBSerialNum])[0][0])
      print type, TxHash, TxDBSerialNum, rawtx

      if type == -51:
        #Credit the buy in to the issuer
        AddressRole = 'issuer'
        Address= rawtx['result']['referenceaddress']

        #Now start updating the crowdsale propertyid balance info
        PropertyID = rawtx['result']['purchasedpropertyid']

        if getdivisible_MP(PropertyID):
          IssuerCreditDebit = int(decimal.Decimal(rawtx['result']['issuertokens'])*decimal.Decimal(1e8))
          BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['purchasedtokens'])*decimal.Decimal(1e8))
        else:
          IssuerCreditDebit = int(decimal.Decimal(rawtx['result']['issuertokens']))
          BalanceAvailableCreditDebit = int(rawtx['result']['purchasedtokens'])

        #If there is an amount to issuer > 0 insert into db otherwise skip
        if IssuerCreditDebit > 0:
          dbExecute("update addressesintxs set AddressRole=%s, BalanceAvailableCreditDebit=%s where Address=%s and propertyid=%s and txdbserialnum=%s and protocol=%s",
                    (AddressRole, IssuerCreditDebit, Address, PropertyID, TxDBSerialNum, Protocol) )
        else:
          #remove old 'bad' entry
          dbExecute("delete from addressesintxs where Address=%s and propertyid=%s and txdbserialnum=%s and protocol=%s", (Address, PropertyID, TxDBSerialNum, Protocol))

        #Participating in crowdsale, update smartproperty history table with the propertyid bought
        dbExecute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, TxDBSerialNum))
        #Trigger update smartproperty json data
        updateProperty(PropertyID, Protocol, TxDBSerialNum)

        #now set the final variables to update addressesintxs/addressbalances with participant crowdsale specific property details
        Address = rawtx['result']['sendingaddress']
        AddressRole = 'participant'

        dbExecute("update addressesintxs set AddressRole=%s, BalanceAvailableCreditDebit=%s where Address=%s and propertyid=%s and txdbserialnum=%s and protocol=%s",
                  (AddressRole, BalanceAvailableCreditDebit, Address, PropertyID, TxDBSerialNum, Protocol) )

      elif type==51 or type == 53:
        #update smart property table
        insertProperty(rawtx, Protocol)
        #pass

dbCommit()


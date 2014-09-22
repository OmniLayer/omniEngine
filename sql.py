import datetime
import decimal
import math
import sys
from rpcclient import *
from mscutils import *
from sqltools import *


def keyByAddress(item):
    return item[0]

def keyByAmount(item):
    return item[1]

def sortSTO(list):
    #First,  sort by Address alphabetically
    list=sorted(list, key=keyByAddress)
    #Second, sort by the amount. Largest to smallest
    list=sorted(list, key=keyByAmount, reverse=True)
    return list

def sendToOwners(Sender, Amount, PropertyID, Protocol, TxDBSerialNum, owners=None):

    if owners == None:
      #get list of owners sorted by most held to least held and by address alphabetically
      owners=sortSTO(dbSelect("select address, balanceavailable from addressbalances where balanceavailable > 0 "
                              "and address != %s and propertyid=%s", (Sender, PropertyID)))
    else:
      #use the addresslist sent to us
      owners=sortSTO(owners)

    #find out how much is actually owned/held in total 
    toDistribute=Amount
    sumTotal=sum([holder[1] for holder in owners])

    #prime first position and set role
    AddressTxIndex=0
    AddressRole='payee'
    Ecosystem=getEcosystem(PropertyID)
    LastHash=gettxhash(TxDBSerialNum)

    #process all holders from the sorted ownerslist
    for holder in owners:
      #who gets it
      Address=holder[0]

      #calculate percentage owed to the holder rounding up always
      amountToSend=int( math.ceil((holder[1]/sumTotal) * Amount))

      #if we sent this amount how much will we have left to send after (used to validate later)
      remaining = toDistribute-amountToSend

      #make sure its a valid amount to send
      if amountToSend > 0:
         #make sure amountToSend is actually available
         if remaining >= 0:
           #send/credit amountToSend to the holder
           amountSent=amountToSend
         else:
           #send/credit whatever is left (toDistribute) to the holder
           amountSent=toDistribute

         #Insert the amountSent record into the addressesintx table?
         dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, amountSent))
         #update balance table
         updateBalance(Address, Protocol, PropertyID, Ecosystem, amountSent, None, None, TxDBSerialNum)

         #make sure we keep track of how much was sent/left to send
         toDistribute-=amountSent

      #/end if amountToSend > 0

      #relative position of the recipiant
      AddressTxIndex+=1

      #no money left to distribute. Done
      if toDistribute == 0:
        break

    #/end for holder in owners


def expireAccepts(Block):
    #find the offers that are ready to expire and credit the 'accepted' amount back to the sellers sale
    expiring=dbSelect("select amountaccepted, saletxdbserialnum from offeraccepts where expireblock < %s and expiredstate=false "
                      "and (dexstate='paid-partial' or dexstate='unpaid')", [Block] )

    #make sure we process all the offers that are expiring
    for offer in expiring:

      #only process if there is anything to process
      amountaccepted=offer[0]
      saletxserialnum=offer[1]

      dbExecute("update activeoffers set amountaccepted=amountaccepted-%s::numeric, amountavailable=amountavailable+%s::numeric "
                "where createtxdbserialnum=%s", (amountaccepted, amountaccepted, saletxserialnum) )

      #credit the offers that are ready to expire back to the sellers balance
      dbExecute("update addressbalances as ab set balanceaccepted=ab.balanceaccepted-%s::numeric "
                "from activeoffers as ao where ab.address=ao.seller and "
                "ab.propertyid = ao.propertyidselling and ao.createtxdbserialnum=%s", 
                (amountaccepted, saletxserialnum) )

    #every block we check any 'active' accepts. If their expire block has passed, we set them expired
    dbExecute("update offeraccepts set expiredstate=true where expireblock < %s and expiredstate=false", [Block] )

def updateAccept(Buyer, Seller, AmountBought, PropertyIDBought, TxDBSerialNum):
    #user has paid for their accept (either partially or in full) update accordingly. 

    #find the accept data for updating
    #saletx=dbSelect("select max(oa.saletxdbserialnum) from offeraccepts as oa inner join activeoffers as ao "
    #                "on (oa.saletxdbserialnum=ao.createtxdbserialnum) "
    #                "where oa.buyer=%s and ao.seller=%s and ao.propertyidselling=%s and oa.expiredstate=false",
    #                (Buyer, Seller, PropertyIDBought) )

    #saletxdbserialnum=saletx[0][0]

    accept=dbSelect("select oa.amountaccepted, oa.amountpurchased, ao.amountaccepted, ao.amountavailable, ao.offerstate, oa.saletxdbserialnum "
                    "from offeraccepts oa inner join activeoffers ao on (oa.saletxdbserialnum=ao.createtxdbserialnum) "
                    "where oa.buyer=%s and ao.seller=%s and ao.propertyidselling=%s "
                    "and oa.dexstate != 'invalid' and oa.dexstate != 'paid-complete' and oa.expiredstate=false", 
                    (Buyer, Seller, PropertyIDBought) )

    buyeraccepted = accept[0][0] - AmountBought
    buyerpurchased= AmountBought + accept[0][1]
    saletxdbserialnum = accept[0][5]

    if buyeraccepted > 0:
      dexstate = 'paid-partial'
    else:
      dexstate = 'paid-complete'
      #can we have a negative amount accepted?  bad math?
 
    #update the buyers 'accept' in the offeraccepts table with the new data
    dbExecute("update offeraccepts as oa set amountaccepted=%s, amountpurchased=%s, dexstate=%s "
              "from activeoffers as ao where oa.saletxdbserialnum=ao.createtxdbserialnum and oa.buyer=%s and ao.seller=%s "
              "and ao.propertyidselling=%s and ao.createtxdbserialnum=%s and oa.dexstate != 'invalid' and "
              "oa.dexstate != 'paid-complete' and oa.expiredstate=false", 
              (buyeraccepted, buyerpurchased, dexstate, Buyer, Seller, PropertyIDBought, saletxdbserialnum) )

    selleraccepted= accept[0][2] - AmountBought
    selleravailable=accept[0][3]

    if selleraccepted == 0 and selleravailable == 0:
      offerstate='sold'
    else:
      offerstate=accept[0][4]

    #update the sellers sale with the information from the buyers successful buy
    dbExecute("update activeoffers as ao set amountaccepted=%s, offerstate=%s, lasttxdbserialnum=%s "
              "from offeraccepts as oa where oa.saletxdbserialnum=ao.createtxdbserialnum "
              "and oa.buyer=%s and ao.seller=%s and ao.propertyidselling=%s and  ao.createtxdbserialnum=%s",
              (selleraccepted, offerstate, TxDBSerialNum, Buyer, Seller, PropertyIDBought, saletxdbserialnum) )

def offerAccept (rawtx, TxDBSerialNum, Block):
    BuyerAddress=rawtx['result']['sendingaddress']
    SellerAddress=rawtx['result']['referenceaddress']
    
    #what did the user accept
    propertyidbuying = rawtx['result']['propertyid']
    #what are they going to have to pay/send to complete. (BTC for now until metadex launch)
    propertyidpaying = 0
    #was it a valid accept, we still insert invalids for displaying to user later
    valid = rawtx['result']['valid']

    #convert accepted amount to non divisible quantity to store in db
    if rawtx['result']['divisible']:
      amountaccepted=int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
    else:
      amountaccepted=int(rawtx['result']['amount'])

    #get the current active dex sale this matches,
    saleinfo=dbSelect("select createtxdbserialnum,timelimit,amountaccepted,amountavailable from activeoffers where seller=%s and offerstate='active'"
                      " and propertyidselling=%s and propertyiddesired=%s",
                      (SellerAddress, propertyidbuying, propertyidpaying) )

    #catch/check if there is a valid sale it's trying to lookup, we can still attribute invalid tx's to the sale they tried to buy
    if len(saleinfo) > 0:
      saletxdbserialnum=saleinfo[0][0]
    else:
      saletxdbserialnum=-1

    if valid:
      #how long does user have to pay
      timelimit=saleinfo[0][1]
      #how much in the sale is currently accepted
      currentamountaccepted=saleinfo[0][2]
      amountavailable=saleinfo[0][3]

      #calculate when the offer should expire
      expireblock=timelimit+Block
      dexstate='unpaid'
      expiredstate='false'
      #update original sale to reflect accept
      currentamountaccepted+=amountaccepted
      amountavailable-=amountaccepted      
      dbExecute("update activeoffers set amountaccepted=%s, amountavailable=%s where  seller=%s and offerstate='active' and propertyidselling=%s and propertyiddesired=%s",
                (currentamountaccepted,amountavailable,SellerAddress,propertyidbuying,propertyidpaying) )
    else:
      dexstate='invalid'
      expiredstate='true'
      expireblock=-1

    #insert the offer
    dbExecute("insert into offeraccepts (buyer, amountaccepted, linkedtxdbserialnum, saletxdbserialnum, block, dexstate, expireblock, expiredstate) "
              "values(%s,%s,%s,%s,%s,%s,%s,%s)", 
              (BuyerAddress, amountaccepted, TxDBSerialNum, saletxdbserialnum, Block, dexstate, expireblock,expiredstate) )

def updatedex(rawtx, TxDBSerialNum):

    Address=rawtx['result']['sendingaddress']
    propertyiddesired=0
    propertyidselling=rawtx['result']['propertyid']

    if getdivisible_MP(propertyidselling):
      amountavailable=int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
    else:
      amountavailable=int(rawtx['result']['amount'])

    #work around for some dex tx's not having a subaction
    if 'subaction' in rawtx['result']:
      subaction=rawtx['result']['subaction']
    elif amountavailable == 0:
      subaction='cancel'
    else:
      subaction='new'

    #Catches, new, update, empty, cancel states from core
    if subaction.lower() == 'cancel':
      State='cancelled'
      #Update any active offers to replace
      dbExecute("update activeoffers set offerstate=%s, LastTxDBSerialNum=%s where seller=%s and offerstate='active' and propertyiddesired=%s and propertyidselling=%s",
                (State, TxDBSerialNum, Address, propertyiddesired, propertyidselling) )
    else:
      #state new/update
      State='replaced'
      #Update any active offers to replace
      dbExecute("update activeoffers set offerstate=%s, LastTxDBSerialNum=%s where seller=%s and offerstate='active' and propertyiddesired=%s and propertyidselling=%s",
                (State, TxDBSerialNum, Address, propertyiddesired, propertyidselling) )
      #insert the new/updated tx as active
      State='active'
      amountaccepted=0

      totalselling=amountavailable

      #convert all btc stuff, need additional logic for metadex  
      amountdesired=int(decimal.Decimal(rawtx['result']['bitcoindesired'])*decimal.Decimal(1e8))
      minimumfee=int(decimal.Decimal(rawtx['result']['fee'])*decimal.Decimal(1e8))

      #rawtx does't have ppc, do the calculation to store
      unitprice=int(amountdesired/amountavailable)

      timelimit=rawtx['result']['timelimit']

      dbExecute("insert into activeoffers (amountaccepted, amountavailable, totalselling, amountdesired, minimumfee, propertyidselling, "
                "propertyiddesired, seller, timelimit, createtxdbserialnum, unitprice, offerstate) values "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (amountaccepted, amountavailable, totalselling, amountdesired, minimumfee, propertyidselling, 
                propertyiddesired, Address, timelimit, TxDBSerialNum, unitprice, State) )

def resetdextable_MP():
      #add code to handle accepts in the dex results
      activesales= getactivedexsells_MP()['result']
      for sale in activesales:
        #0 for btc for now all sales use btc
        propertyiddesired=0
        propertyidselling=sale['propertyid']

        if getdivisible_MP(propertyidselling):
          amountaccepted=int(decimal.Decimal(sale['amountaccepted'])*decimal.Decimal(1e8))
          amountavailable=int(decimal.Decimal(sale['amountavailable'])*decimal.Decimal(1e8))
        else:
          amountaccepted=int(sale['amountaccepted'])
          amountavailable=int(sale['amountavailable'])

        totalselling=amountaccepted

        #convert all btc stuff, need additional logic for metadex  
        amountdesired=int(decimal.Decimal(sale['bitcoindesired'])*decimal.Decimal(1e8))
        minimumfee=int(decimal.Decimal(sale['minimumfee'])*decimal.Decimal(1e8))
        unitprice=int(decimal.Decimal(sale['unitprice'])*decimal.Decimal(1e8))
        
        seller=sale['seller']
        timelimit=sale['timelimit']
        createtxdbserialnum=gettxdbserialnum(sale['txid'])
        offerstate='active'

        dbExecute("insert into activeoffers (amountaccepted, amountavailable, totalselling, amountdesired, minimumfee, propertyidselling, "
                  "propertyiddesired, seller, timelimit, createtxdbserialnum, unitprice, offerstate) values "
                  "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                  (amountaccepted, amountavailable, totalselling, amountdesired, minimumfee, propertyidselling, propertyiddesired,
                   seller, timelimit, createtxdbserialnum, unitprice, offerstate) )    

def syncAddress(Address, Protocol):
    #sync address balance in db to match core's balance
    #mainly used for exodus dev msc distribution sync but is abstracted for any address

    baldata=getallbalancesforaddress_MP(Address)['result']
    DExSales=getactivedexsells_MP()['result']

    for property in baldata:
      PropertyID=property['propertyid']

      #get available/reserved balances
      if getdivisible_MP(PropertyID):
        Available=int(decimal.Decimal(property['balance'])*decimal.Decimal(1e8))
        Reserved=int(decimal.Decimal(property['reserved'])*decimal.Decimal(1e8))
      else:
        Available=int(property['balance'])
        Available=int(property['reserved'])

      #find accepted balances (if exists)
      for x in DExSales:
        if x['seller'] == Address and x['propertyid']==PropertyID:
          Accepted = x['amountaccepted']
          break
        else:
          Accepted=0

      #check for the address
      rows=dbSelect("select address from AddressBalances where address=%s and Protocol=%s and propertyid=%s",
                    (Address, Protocol, PropertyID) )

      if len(rows) == 0:
        Ecosystem=getEcosystem(PropertyID)
        #address not in database, insert
        dbExecute("INSERT into AddressBalances "
                  "(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted) "
                  "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                  (Address, Protocol, PropertyID, Ecosystem, Available, Reserved, Accepted) )
      else:
        #address in database update
        dbExecute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s, BalanceAccepted=%s where address=%s and PropertyID=%s",
                  (Available, Reserved, Accepted, Address, PropertyID) )

      


def resetbalances_MP():
    #for now sync / reset balance data from mastercore balance list
    Protocol="Mastercoin"

    #get DEx sales to process 'accepted' amounts  
    DExSales=getactivedexsells_MP()

    #Find all known properties in mastercore
    for property in listproperties_MP()['result']:
      PropertyID = property['propertyid']
      Ecosystem=getEcosystem(PropertyID)
      #if PropertyID == 2 or ( PropertyID >= 2147483651 and PropertyID <= 4294967295 ):
      #  Ecosystem= "Test"
      #else:
      #  Ecosystem= "Production"
      bal_data=getallbalancesforid_MP(PropertyID)

      #Check each address and get balance info
      for addr in bal_data['result']:
        Address=addr['address']

        #find reserved balance (if exists)
        for x in DExSales['result']:
          if x['seller'] == Address and x['propertyid']==PropertyID:
            accept = x['amountaccepted']
            break
          else:
            accept=0

        if property['divisible']:
          BalanceAvailable=int(decimal.Decimal(addr['balance'])*decimal.Decimal(1e8))
          BalanceReserved=int(decimal.Decimal(addr['reserved'])*decimal.Decimal(1e8))
          BalanceAccepted=int(decimal.Decimal(accept)*decimal.Decimal(1e8))
        else:
          BalanceAvailable=int(addr['balance'])
          BalanceReserved=int(addr['reserved'])
          BalanceAccepted=int(decimal.Decimal(accept))

        rows=dbSelect("select address from AddressBalances where address=%s and Protocol=%s and propertyid=%s", 
                      (Address, Protocol, PropertyID) )

        if len(rows) == 0:
          #address not in database, insert
          dbExecute("INSERT into AddressBalances "
                    "(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted) )
        else:
          #address in database update
          dbExecute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s, BalanceAccepted=%s where address=%s and PropertyID=%s", 
                    (BalanceAvailable, BalanceReserved, BalanceAccepted, Address, PropertyID) )

def checkbalances_MP():
    #for now sync / reset balance data from mastercore balance list
    Protocol="Mastercoin"

    #get DEx sales to process 'accepted' amounts
    DExSales=getactivedexsells_MP()

    retval={}

    #Find all known properties in mastercore
    for property in listproperties_MP()['result']:
      PropertyID = property['propertyid']
      Ecosystem=getEcosystem(PropertyID)
      #if PropertyID == 2 or ( PropertyID >= 2147483651 and PropertyID <= 4294967295 ):
      #  Ecosystem= "Test"
      #else:
      #  Ecosystem= "Production"
      bal_data=getallbalancesforid_MP(PropertyID)

      #Check each address and get balance info
      for addr in bal_data['result']:
        Address=addr['address']

        #find reserved balance (if exists)
        for x in DExSales['result']:
          if x['seller'] == Address and x['propertyid']==PropertyID:
            accept = x['amountaccepted']
            break
          else:
            accept=0

        if property['divisible']:
          BalanceAvailable=int(decimal.Decimal(addr['balance'])*decimal.Decimal(1e8))
          BalanceReserved=int(decimal.Decimal(addr['reserved'])*decimal.Decimal(1e8))
          BalanceAccepted=int(decimal.Decimal(accept)*decimal.Decimal(1e8))
        else:
          BalanceAvailable=int(addr['balance'])
          BalanceReserved=int(addr['reserved'])
          BalanceAccepted=int(decimal.Decimal(accept))

        rows=dbSelect("select address,BalanceAvailable,balancereserved,balanceaccepted from AddressBalances "
                      "where address=%s and Protocol=%s and propertyid=%s",
                      (Address, Protocol, PropertyID) )

        #check db for None/Null returns and convert to match core 0 output
        if rows[0][1] == None:
          dbBalanceAvailable = 0
        else:
          dbBalanceAvailable = int(rows[0][1])

        if rows[0][2] == None:
          dbBalanceReserved = 0
        else:
          dbBalanceReserved = int(rows[0][2])

        if rows[0][3] == None:
          dbBalanceAccepted = 0
        else:
          dbBalanceAccepted = int(rows[0][3])

        item={}
        if len(rows) == 0:
          #address not in database, insert
          item[PropertyID] ={'Address':address, 'bal':{'Status': 'Missing', 'PropertyID': PropertyID, 'BalanceAvailable':BalanceAvailable,'BalanceReserved': BalanceReserved,'BalanceAccepted':BalanceAccepted }}
          #add the missing/incorrect item to our list to return
          retval.update(item) 
        else:
          #address in database update
          if BalanceAvailable != dbBalanceAvailable:
            item[PropertyID] ={'Address':Address, 'bal':{'Status': 'Mismatch', 'PropertyID': PropertyID, 'BalanceAvailable':BalanceAvailable, 'dbBalanceAvailable': dbBalanceAvailable, 
                                'dbBalanceReserved': dbBalanceReserved, 'BalanceReserved': BalanceReserved,
                                'dbBalanceAccepted':dbBalanceAccepted, 'BalanceAccepted':BalanceAccepted }}
            #add the missing/incorrect item to our list to return
            retval.update(item) 
          elif BalanceReserved != dbBalanceReserved:
            item[PropertyID] ={'Address':Address, 'bal':{'Status': 'Mismatch', 'PropertyID': PropertyID, 'BalanceAvailable':BalanceAvailable, 'dbBalanceAvailable': dbBalanceAvailable,
                                'dbBalanceReserved': dbBalanceReserved, 'BalanceReserved': BalanceReserved,
                                'dbBalanceAccepted':dbBalanceAccepted, 'BalanceAccepted':BalanceAccepted }}
            #add the missing/incorrect item to our list to return
            retval.update(item) 
          elif BalanceAccepted != dbBalanceAccepted:
            item[PropertyID] ={'Address':Address, 'bal':{'Status': 'Mismatch', 'PropertyID': PropertyID, 'BalanceAvailable':BalanceAvailable, 'dbBalanceAvailable': dbBalanceAvailable,
                                'dbBalanceReserved': dbBalanceReserved, 'BalanceReserved': BalanceReserved,
                                'dbBalanceAccepted':dbBalanceAccepted, 'BalanceAccepted':BalanceAccepted }}
            #add the missing/incorrect item to our list to return
            retval.update(item) 

    return retval



def updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxDBSerialNum):
    
      print "Updating balance state:"
      print "Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, TxDBSerialNum"
      print Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxDBSerialNum

      rows=dbSelect("select BalanceAvailable, BalanceReserved, BalanceAccepted "
                    "from AddressBalances where address=%s and Protocol=%s and propertyid=%s",
                    (Address, Protocol, PropertyID) )

      if len(rows) == 0:
        try:
          BalanceAvailable=int(BalanceAvailable)
        except (ValueError, TypeError):
          BalanceAvailable=0

        try:
          BalanceReserved=int(BalanceReserved)
        except (ValueError, TypeError):
          BalanceReserved=0

        try:
          BalanceAccepted=int(BalanceAccepted)
        except (ValueError, TypeError):
          BalanceAccepted=0

        #address not in database, insert
        dbExecute("INSERT into AddressBalances "
                    "(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxDBSerialNum) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s, %s)",
                    (Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxDBSerialNum) )
      else:
        #address in database update
        #check if null values and default to no change on update
        dbAvail=rows[0][0]
        dbResvd=rows[0][1]
        dbAccpt=rows[0][2]

        try:
          BalanceAvailable=int(BalanceAvailable)+dbAvail
        except (ValueError, TypeError):
          try:
            BalanceAvailable=dbAvail+0
          except (ValueError, TypeError): 
            BalanceAvailable=0

        try:
          BalanceReserved=int(BalanceReserved)+dbResvd
        except (ValueError, TypeError):
          try:
            BalanceReserved=dbResvd+0 
          except (ValueError, TypeError):  
            BalanceReserved=0

        try:
          BalanceAccepted=int(BalanceAccepted)+dbAccpt
        except (ValueError, TypeError):
          try:
            BalanceAccepted=dbAccpt+0
          except (ValueError, TypeError):
            BalanceAccepted=0

        dbExecute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s, BalanceAccepted=%s, LastTxDBSerialNum=%s where address=%s and PropertyID=%s and Protocol=%s",
                  (BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxDBSerialNum, Address, PropertyID, Protocol) )


def expireCrowdsales(BlockTime, Protocol):
    #find the offers that are ready to expire and credit the 'accepted' amount back to the sellers sale
    expiring=dbSelect("select propertyid from smartproperties as sp inner join transactions as tx on "
                      "(sp.createtxdbserialnum=tx.txdbserialnum) where tx.txtype=51 and "
                      "cast(propertydata::json->>'endedtime' as numeric) < %s and propertydata::json->>'active'='true'", [BlockTime])

    #Process all the crowdsales that should have expired by now
    for property in expiring:
      updateProperty(property[0], Protocol)


def updateProperty(PropertyID, Protocol, LastTxDBSerialNum=None):
    PropertyDataJson=getproperty_MP(PropertyID)
    rawtx=gettransaction_MP(PropertyDataJson['result']['creationtxid'])
    TxType = get_TxType(rawtx['result']['type'])
    rawprop = PropertyDataJson['result']

    if TxType == 51 or TxType == 53:
      #get additional json info for crowdsales
       rawprop = dict(rawprop.items() + getcrowdsale_MP(PropertyID)['result'].items())
    elif TxType > 53 and TxType < 57:
       rawprop = dict(rawprop.items() + getgrants_MP(PropertyID)['result'].items())

    #if we where called with a tx update that otherwise jsut update json (expired by time update)
    if LastTxDBSerialNum == None:
      dbExecute("update smartproperties set PropertyData=%s "
                "where Protocol=%s and PropertyID=%s",
                (json.dumps(rawprop), Protocol, PropertyID))
    else:
      dbExecute("update smartproperties set LastTxDBSerialNum=%s, PropertyData=%s "
                "where Protocol=%s and PropertyID=%s",
                (LastTxDBSerialNum, json.dumps(rawprop), Protocol, PropertyID))


def insertProperty(rawtx, Protocol, PropertyID=None):
    #only insert valid updates. ignore invalid data?
    if rawtx['result']['valid']:

      TxType = get_TxType(rawtx['result']['type'])

      #User PropertyID from tx unless specifically overwritten
      if PropertyID == None:
        PropertyID = rawtx['result']['propertyid']
    
      PropertyDataJson = getproperty_MP(PropertyID)
      rawprop = PropertyDataJson['result'] 

      if TxType == 51 or TxType == 53:
        #get additional json info for crowdsales
        rawprop = dict(rawprop.items() + getcrowdsale_MP(PropertyID)['result'].items())        
      elif TxType > 53 and TxType < 57:
        rawprop = dict(rawprop.items() + getgrants_MP(PropertyID)['result'].items())


      Issuer = rawprop['issuer']
      Ecosystem = getEcosystem(PropertyID)
      lasthash = rawtx['result']['txid']
      LastTxDBSerialNum = gettxdbserialnum(lasthash)
      createhash = rawprop['creationtxid']
      CreateTxDBSerialNum = gettxdbserialnum(createhash)

      PropertyName = rawprop['name']
      #propertyurl = rawprop['url']
      if rawprop['divisible']:
        PropertyType = 1
      else:
        PropertyType = 0
      PropertyData = rawprop['data']
      PropertyCategory = rawprop['category']
      PropertySubcategory =rawprop['subcategory'] 

      #, PrevPropertyID bigint null default 0
      #, PropertyServiceURL varchar(256) null

      #do the update/insert, once we have the final structure defined
      ROWS=dbSelect("select * from smartproperties where Protocol=%s and PropertyID=%s", (Protocol, PropertyID))
      if len(ROWS) > 0:
        #Its already there, update it and insert into history table
        dbExecute("update smartproperties set Issuer=%s, Ecosystem=%s, CreateTxDBSerialNum=%s, LastTxDBSerialNum=%s, "
                  "PropertyName=%s, PropertyType=%s, PropertyCategory=%s, PropertySubcategory=%s, PropertyData=%s "
                  "where Protocol=%s and PropertyID=%s",
                  (Issuer, Ecosystem, CreateTxDBSerialNum, LastTxDBSerialNum, PropertyName, PropertyType, PropertyCategory, 
                   PropertySubcategory, json.dumps(rawprop), Protocol, PropertyID))
        #insert this tx into the history table
        dbExecute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, LastTxDBSerialNum))
      else:
        #doesn't exist, insert
        dbExecute("insert into SmartProperties"
                  "(Issuer, Ecosystem, CreateTxDBSerialNum, LastTxDBSerialNum, PropertyName, PropertyType, "
                  "PropertyCategory, PropertySubcategory, PropertyData, Protocol, PropertyID )"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Issuer, Ecosystem, CreateTxDBSerialNum, LastTxDBSerialNum, PropertyName, PropertyType, PropertyCategory, 
                   PropertySubcategory, json.dumps(rawprop), Protocol, PropertyID))
        #insert this tx into the history table
        dbExecute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, LastTxDBSerialNum))

def insertTxAddr(rawtx, Protocol, TxDBSerialNum, Block):
    TxHash = rawtx['result']['txid']

    if Protocol == "Bitcoin":
      PropertyID=0
      Ecosystem=None
      #process all outputs
      for output in rawtx['result']['vout']:
        #Make sure we have readable output addresses to actually use
        if 'addresses' in output['scriptPubKey']:
          AddressRole="recipient"
          AddressTxIndex=output['n']
          #store values as satoshi/willits etc''. Client converts
          BalanceAvailableCreditDebit=int(decimal.Decimal(output['value'])*decimal.Decimal("1e8"))
          #multisigs have more than 1 address, make sure we find/credit all multisigs for a tx
          for addr in output['scriptPubKey']['addresses']:
            dbExecute("insert into addressesintxs "
                      "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit)"
                      "values(%s, %s, %s, %s, %s, %s, %s)",
                      (addr, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit))
            updateBalance(addr, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, 0, 0, TxDBSerialNum)


      #process all inputs, Start AddressTxIndex=0 since inputs don't have a Index number in json and iterate for each input
      AddressTxIndex=0
      for input in rawtx['result']['vin']:
        #check if we have previous input txids we need to lookup or if its a coinbase (newly minted coin ) which needs to be skipped
        if 'txid' in input:
          AddressRole="sender"
          #existing json doesn't have raw address only prev tx. Get prev tx to decipher address/values
          prevtxhash=input['txid']
          prevtxindex=input['vout']
          prevtx=getrawtransaction(prevtxhash)

          #get prev txdbserial num and update output/recipient of previous tx for utxo stuff
          LinkedTxDBSerialNum=gettxdbserialnum(prevtxhash)

          dbExecute("update addressesintxs set LinkedTxDBSerialNum=%s where protocol=%s and txdbserialnum=%s"
                    " and addresstxindex=%s and addressrole='recipient'",
                    (TxDBSerialNum, Protocol, LinkedTxDBSerialNum, prevtxindex) )

          BalanceAvailableCreditDebit=int(decimal.Decimal(prevtx['result']['vout'][input['vout']]['value'])*decimal.Decimal("1e8")*decimal.Decimal(-1))
          #BalanceAvailableCreditDebit=int(prevtx['result']['vout'][input['vout']]['value'] * 1e8 * -1)
          #multisigs have more than 1 address, make sure we find/credit all multisigs for a tx
          for addr in prevtx['result']['vout'][input['vout']]['scriptPubKey']['addresses']:
            dbExecute("insert into addressesintxs "
                      "(Address, PropertyID, Protocol, TxDBSerialNum, LinkedTxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit)"
                      "values(%s, %s, %s, %s, %s, %s, %s, %s)",
                      (addr, PropertyID, Protocol, TxDBSerialNum, LinkedTxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit))
            updateBalance(addr, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, 0, 0, TxDBSerialNum)
          AddressTxIndex+=1

    elif Protocol == "Mastercoin":
      AddressTxIndex=0
      AddressRole="sender"
      type=get_TxType(rawtx['result']['type'])
      BalanceAvailableCreditDebit=None
      BalanceReservedCreditDebit=None
      BalanceAcceptedCreditDebit=None
      Address = rawtx['result']['sendingaddress']
      #PropertyID=rawtx['result']['propertyid']

      #Check if we are a DEx Purchase/payment. Format is a littler different and variables below would fail if we tried. 
      if type != -22:
        PropertyID= rawtx['result']['propertyid']
        Ecosystem=getEcosystem(PropertyID) 
        Valid=rawtx['result']['valid']

        if rawtx['result']['divisible']:
          value=int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
        else:
          value=int(rawtx['result']['amount'])
        value_neg=(value*-1)


      if type == 0:
        #Simple Send
        BalanceAvailableCreditDebit=value_neg 

	#debit the sender
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
  
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

	#credit the receiver
        Address = rawtx['result']['referenceaddress']
	AddressRole="recipient"
        BalanceAvailableCreditDebit=value

      #elif type == 2:
	#Restricted Send does nothing yet?

      elif type == 3:
        #Send To Owners
        if Valid:
           sendToOwners(Address, value, PropertyID, Protocol, TxDBSerialNum)
        #Debit the sender
        BalanceAvailableCreditDebit=value_neg

      elif type == 20:
        #DEx Sell Offer
        #Move the amount from Available balance to reserved for Offer
        ##Sell offer cancel doesn't display an amount from core, not sure what we do here yet
        AddressRole='seller'
        BalanceAvailableCreditDebit = value_neg
        BalanceReservedCreditDebit = value

        #Update our DEx tables if its a valid dex sale
        if rawtx['result']['valid']:
          updatedex(rawtx, TxDBSerialNum)

      #elif type == 21:
        #MetaDEx: Offer/Accept one Master Protocol Coins for another
        #return

      elif type == 22:
        #DEx Accept Offer

        #insert record for the buyer
        AddressRole='buyer'
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

        #Process records for the seller
        AddressRole='seller'
        Address = rawtx['result']['referenceaddress']

        #update the accepted offers (actually track invalid offers for reporting as well)
        offerAccept(rawtx, TxDBSerialNum, Block)

        #track the amount accepted from the seller
        BalanceAcceptedCreditDebit = value

        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
        #credit the sellers 'accepted' balance
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

        #we processed everything for this tx, return
        return

      elif type == -22:
        #DEx Accept Payment

        Buyer =  Address
        #process all purchases in the transaction 
        for payment in rawtx['result']['purchases']:

          Seller = payment['referenceaddress']
          PropertyIDBought = payment['propertyid']
          Valid=payment['valid']

          #Right now payments are only in btc
          #we already insert btc payments in btc processing might need to skip this
          PropertyIDPaid = 0
          #AddressTxIndex =  Do we need to change this?

          if getdivisible_MP(PropertyIDBought):
            AmountBought=int(decimal.Decimal(payment['amountbought'])*decimal.Decimal(1e8))
          else:
            AmountBought=int(payment['amountbought'])
          AmountBoughtNeg=(AmountBought * -1)

          #if (PropertyIDPaid == 0 ) or getdivisible_MP(PropertyIDPaid):
          #  AmountPaid=int(decimal.Decimal(payment['amountpaid'])*decimal.Decimal(1e8))
          #else:
          #  AmountPaid=int(payment['amountpaid'])
          #AmountPaidNeg=(AmountPaid * -1)

          #deduct payment from buyer
          #AddressRole = 'buyer'
          #BalanceAvailableCreditDebit=AmountPaidNeg
          #row={'Address': Sender, 'PropertyID': PropertyIDPaid, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
          #     'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
          #     'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
          #csvwb.writerow(row)

          #Credit payment to seller
          #AddressRole = 'seller'
          #BalanceAvailableCreditDebit=AmountPaid
          #row={'Address': Receiver, 'PropertyID': PropertyIDPaid, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
          #     'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
          #     'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
          #csvwb.writerow(row)

          #deduct tokens from seller
          AddressRole = 'seller'
          BalanceReservedCreditDebit=AmountBoughtNeg
          Ecosystem=getEcosystem(PropertyIDBought)
          dbExecute("insert into addressesintxs "
                    "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (Seller, PropertyIDBought, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

          if Valid:
            #deduct the amount bought from both reserved and accepted fields, since we track it twice to match core (it only tracks reserved)
            BalanceAcceptedCreditDebit=AmountBoughtNeg
            updateBalance(Seller, Protocol, PropertyIDBought, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)
            #reset it to null to not screw up next insert
            BalanceAcceptedCreditDebit=None

          #Credit tokens tco buyer and reduce their accepted amount by amount bought
          AddressRole = 'buyer'
          BalanceAvailableCreditDebit=AmountBought
          BalanceReservedCreditDebit=None
          dbExecute("insert into addressesintxs "
                    "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (Buyer, PropertyIDBought, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

          if Valid:
            updateBalance(Buyer, Protocol, PropertyIDBought, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)
            updateAccept(Buyer, Seller, BalanceAvailableCreditDebit, PropertyIDBought, TxDBSerialNum)

          #end //for payment in rawtx['result']['purchases']

        #We've updated all the records in the DEx payment, don't let the last write command run, not needed
        return

      elif type == 50:
        #Fixed Issuance, create property
        AddressRole = "issuer"
        #update smart property table
        insertProperty(rawtx, Protocol)
     
      elif type == 51:
        AddressRole = "issuer"
        #update smart property table
        insertProperty(rawtx, Protocol)

      elif type == -51:
        #First deduct the amount the participant sent to 'buyin'  (BTC Amount might need to be excluded?)
        AddressRole = 'participant'
        BalanceAvailableCreditDebit = value_neg

        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

        #Credit the buy in to the issuer
        AddressRole = 'issuer'
        BalanceAvailableCreditDebit = value
        Address= rawtx['result']['referenceaddress']
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

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
          dbExecute("insert into addressesintxs "
                    "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, IssuerCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
          if Valid:
            updateBalance(Address, Protocol, PropertyID, Ecosystem, IssuerCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

        #Participating in crowdsale, update smartproperty history table with the propertyid bought
        dbExecute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, TxDBSerialNum))
        #Trigger update smartproperty json data 
        updateProperty(PropertyID, Protocol, TxDBSerialNum)

        #now set the final variables to update addressesintxs/addressbalances with participant crowdsale specific property details
        Address = rawtx['result']['sendingaddress']
        AddressRole = 'participant'
 
      #elif type == 52:
        #promote crowdsale does what?

      elif type == 53:
        #Close Crowdsale
        AddressRole = "issuer"
        BalanceAvailableCreditDebit=None

        #update smart property table
        insertProperty(rawtx, Protocol)

      elif type == 54:
        AddressRole = "issuer"
        BalanceAvailableCreditDebit=0

        #update smart property table
        insertProperty(rawtx, Protocol)

      elif type == 55:
        AddressRole = "issuer"
        BalanceAvailableCreditDebit=value

        #update smart property table
        insertProperty(rawtx, Protocol)

        #update balanace table
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

      elif type == 56:
        AddressRole = "issuer"
        BalanceAvailableCreditDebit=value_neg

        #update smart property table
        insertProperty(rawtx, Protocol)

        #update balanace table
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)

      #write output of the address details
      dbExecute("insert into addressesintxs "
                "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

      if Valid:
        updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxDBSerialNum)


def insertTx(rawtx, Protocol, blockheight, seq, TxDBSerialNum):
    TxHash = rawtx['result']['txid']
    TxBlockTime = datetime.datetime.utcfromtimestamp(rawtx['result']['blocktime'])
    TxErrorCode = rawtx['error']
    TxSeqInBlock= seq
    TxBlockNumber = blockheight
    #TxDBSerialNum = dbserialnum

    if Protocol == "Bitcoin":
      #Bitcoin is only simple send, type 0
      TxType=0
      TxVersion=rawtx['result']['version']
      TxState= "valid"
      Ecosystem= None
      TxSubmitTime = datetime.datetime.utcfromtimestamp(rawtx['result']['time'])

    elif Protocol == "Mastercoin":
      #currently type a text output from mastercore 'Simple Send' and version is unknown
      TxType= get_TxType(rawtx['result']['type'])
      TxVersion=0
      #!!temp workaround, Need to update for DEx Purchases after conversation with MasterCore team
      if TxType == -22:
        TxState=getTxState(rawtx['result']['purchases'][0]['valid'])
        Ecosystem=getEcosystem(rawtx['result']['purchases'][0]['propertyid'])
      else:
        TxState= getTxState(rawtx['result']['valid'])
        Ecosystem=getEcosystem(rawtx['result']['propertyid'])

      #Use block time - 10 minutes to approx
      #TxSubmitTime = TxBlockTime-datetime.timedelta(minutes=10)
      TxSubmitTime=None
      #if rawtx['result']['propertyid'] == 2 or ( rawtx['result']['propertyid'] >= 2147483651 and rawtx['result']['propertyid'] <= 4294967295 ):
      #  Ecosystem= "Test"
      #else:
      #  Ecosystem= "Production"

    else:
      print "Wrong Protocol? Exiting, goodbye."
      exit(1)

    if TxDBSerialNum == -1:
        dbExecute("INSERT into transactions "
                  "(TxHash, Protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock ) "
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                  (TxHash, Protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock))
    else:
        dbExecute("INSERT into transactions "
                  "(TxHash, Protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock, TxDBSerialNum ) "
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                  (TxHash, Protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock, TxDBSerialNum))

    serial=dbSelect("Select TxDBSerialNum from transactions where txhash=%s and protocol=%s", (TxHash, Protocol))
    dbExecute("insert into txjson (txdbserialnum, protocol, txdata) values (%s,%s,%s)", (serial[0]['txdbserialnum'], Protocol, json.dumps(rawtx['result'])) )
    return serial[0]['txdbserialnum']


def insertBlock(block_data, Protocol, block_height, txcount):
    BlockTime = datetime.datetime.utcfromtimestamp(block_data['result']['time'])
    version = block_data['result']['version'];
    if block_height > 0:
      prevblockhash = block_data['result']['previousblockhash'];
    else:
      prevblockhash = '0000000000000000000000000000000000000000000000000000000000000000'
    merkleroot = block_data['result']['merkleroot'];
    blockhash = block_data['result']['hash'];
    bits = block_data['result']['bits'];
    nonce = block_data['result']['nonce'];
    size = block_data['result']['size'];
    BlockNumber=block_height

    dbExecute("INSERT into Blocks"
              "(BlockNumber, Protocol, BlockTime, version, blockhash, prevblock, merkleroot, bits, nonce, size, txcount)"
              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
              (BlockNumber, Protocol, BlockTime, version, blockhash, prevblockhash, merkleroot, bits, nonce, size, txcount))


def gettxdbserialnum(txhash):
    ROWS=dbSelect("select txdbserialnum from transactions where txhash=%s",[txhash])
    if len(ROWS)==0:
        return -1
    else:
        return ROWS[0][0]

def gettxhash(txdbserialnum):
   ROWS=dbSelect("select txhash from transactions where txdbserialnum=%s",[txdbserialnum])
   if len(ROWS)==0:
        return -1
   else:
        return ROWS[0][0]

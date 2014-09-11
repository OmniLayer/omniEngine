import psycopg2, psycopg2.extras
import datetime
import decimal
import sys
from rpcclient import *
from mscutils import *

def sql_connect():
    global con
    USER=getpass.getuser()
    try:
      with open('/home/'+USER+'/.omni/sql.conf') as fp:
        DBPORT="5432"
        for line in fp:
          #print line
          if line.split('=')[0] == "sqluser":
            DBUSER=line.split('=')[1].strip()
          elif line.split('=')[0] == "sqlpassword":
            DBPASS=line.split('=')[1].strip()
          elif line.split('=')[0] == "sqlconnect":
            DBHOST=line.split('=')[1].strip()
          elif line.split('=')[0] == "sqlport":
            DBPORT=line.split('=')[1].strip()
          elif line.split('=')[0] == "sqldatabase":
            DBNAME=line.split('=')[1].strip()
    except IOError as e:
      response='{"error": "Unable to load sql config file. Please Notify Site Administrator"}'
      return response

    try:     
        con = psycopg2.connect(database=DBNAME, user=DBUSER, password=DBPASS, host=DBHOST, port=DBPORT)
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    	return cur
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e    
        sys.exit(1)


#Prime the DB Connection
dbc=sql_connect()


def dbSelect(statement, values):
    try:
        dbc.execute(statement, values)
        ROWS = dbc.fetchall()
        return ROWS
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)

def dbExecute(statement, values):
    try:
        dbc.execute(statement, values)
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)

def dbCommit():
    try:
        con.commit()
    except psycopg2.DatabaseError, e:
        if con:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)

def dbRollback():
    if con:
       con.rollback()
       return 1
    else:
       return 0

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
          if x['seller'] == Address:
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
                    "(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved) )
        else:
          #address in database update
          dbExecute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s where address=%s and PropertyID=%s", 
                    (BalanceAvailable, BalanceReserved, Address, PropertyID) )

def updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxHash):
    
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
                    "(Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxHash) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s, %s)",
                    (Address, Protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxHash) )
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

        dbExecute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s, BalanceAccepted=%s, LastTxHash=%s where address=%s and PropertyID=%s and Protocol=%s",
                  (BalanceAvailable, BalanceReserved, BalanceAccepted, LastTxHash, Address, PropertyID, Protocol) )


def insertProperty(rawtx, Protocol):
    #only insert valid updates. ignore invalid data?
    if rawtx['result']['valid']:
      PropertyID = rawtx['result']['propertyid']
      TxType = get_TxType(rawtx['result']['type'])
    
      PropertyDataJson = getproperty_MP(PropertyID)
      rawprop = PropertyDataJson['result'] 

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

def insertTxAddr(rawtx, Protocol, TxDBSerialNum):
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
            updateBalance(addr, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, 0, 0, TxHash)


      #process all inputs, Start AddressTxIndex=0 since inputs don't have a Index number in json and iterate for each input
      AddressTxIndex=0
      for input in rawtx['result']['vin']:
        #check if we have previous input txids we need to lookup or if its a coinbase (newly minted coin ) which needs to be skipped
        if 'txid' in input:
          AddressRole="sender"
          #existing json doesn't have raw address only prev tx. Get prev tx to decipher address/values
          prevtx=getrawtransaction(input['txid'])

          #get prev txdbserial num and update output/recipient of previous tx for utxo stuff
          LinkedTxDBSerialNum=gettxdbserialnum(input['txid'])
          prevtxindex=input['vout']
          dbExecute("update addressesintxs set LinkedTxDBSerialNum=%s where protocol=%s and txdbserialnum=%s"
                    " and addresstxindex=%s and addressrole='recipient'",
                    (LinkedTxDBSerialNum, Protocol, TxDBSerialNum, prevtxindex) )

          BalanceAvailableCreditDebit=int(decimal.Decimal(prevtx['result']['vout'][input['vout']]['value'])*decimal.Decimal("1e8")*decimal.Decimal(-1))
          #BalanceAvailableCreditDebit=int(prevtx['result']['vout'][input['vout']]['value'] * 1e8 * -1)
          #multisigs have more than 1 address, make sure we find/credit all multisigs for a tx
          for addr in prevtx['result']['vout'][input['vout']]['scriptPubKey']['addresses']:
            dbExecute("insert into addressesintxs "
                      "(Address, PropertyID, Protocol, TxDBSerialNum, LinkedTxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit)"
                      "values(%s, %s, %s, %s, %s, %s, %s, %s)",
                      (addr, PropertyID, Protocol, TxDBSerialNum, LinkedTxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit))
            updateBalance(addr, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, 0, 0, TxHash)
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
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

	#credit the receiver
        Address = rawtx['result']['referenceaddress']
	AddressRole="recipient"
        BalanceAvailableCreditDebit=value

      #elif type == 2:
	#Restricted Send does nothing yet?

      #elif type == 3:
        #Send To Owners
	#Do something smart
        #return

      elif type == 20:
        #DEx Sell Offer
        #Move the amount from Available balance to reserved for Offer
        ##Sell offer cancel doesn't display an amount from core, not sure what we do here yet
        AddressRole='seller'
        BalanceAvailableCreditDebit = value_neg
        BalanceReservedCreditDebit = value

      #elif type == 21:
        #MetaDEx: Offer/Accept one Master Protocol Coins for another
        #return

      elif type == 22:
        #DEx Accept Offer
        #Update the amount  Reserved for Accept

        #update the buyer
        AddressRole='buyer'
        BalanceAcceptedCreditDebit = value
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))
  
        AddressRole='seller'
        Address = rawtx['result']['referenceaddress']
        #credit the sellers 'accepted' balance
        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

        #track the address as part of the tx, but it has no balance changing values for the addressesintx table
        BalanceAcceptedCreditDebit = None

        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

        #we processed everything for this tx, return
        return

      elif type == -22:
        #DEx Accept Payment

        Sender =  Address
        #process all purchases in the transaction 
        for payment in rawtx['result']['purchases']:

          Receiver = payment['referenceaddress']
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
          Ecosystem=getEctosystem(PropertyIDBought)
          dbExecute("insert into addressesintxs "
                    "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (Receiver, PropertyIDBought, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

          if Valid:
            #deduct the amount bought from both reserved and accepted fields, since we track it twice to match core (it only tracks reserved)
            BalanceAcceptedCreditDebit=AmountBoughtNeg
            updateBalance(Receiver, Protocol, PropertyIDBought, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)
            #reset it to null to not screw up next insert
            BalanceAcceptedCreditDebit=None

          #Credit tokens tco buyer and reduce their accepted amount by amount bought
          AddressRole = 'buyer'
          BalanceAvailableCreditDebit=AmountBought
          BalanceReservedCreditDebut=None
          dbExecute("insert into addressesintxs "
                    "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (Sender, PropertyIDBought, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

          if Valid:
            updateBalance(Sender, Protocol, PropertyIDBought, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

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
        #Participating in crowdsale
        dbExecute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, TxDBSerialNum))

        #First deduct the amount the participant sent to 'buyin'  (BTC Amount might need to be excluded?)
        AddressRole = 'participant'
        BalanceAvailableCreditDebit = value_neg

        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

        #Credit the buy in to the issuer
        AddressRole = 'issuer'
        BalanceAvailableCreditDebit = value
        Address= rawtx['result']['referenceaddress']
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

        #Now start updating the crowdsale propertyid balance info
        PropertyID = rawtx['result']['purchasedpropertyid']

        #add additional functionalty to check/credit the issue when there is a % bonus to issuer
        cstx = getcrowdsale_MP(PropertyID)
        if cstx['result']['percenttoissuer'] > 0:
          if getdivisible_MP(PropertyID):
            BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(cstx['result']['tokensperunit'])*(decimal.Decimal(cstx['result']['percenttoissuer'])/decimal.Decimal(100))*decimal.Decimal(1e8))
          else:  
            BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(cstx['result']['tokensperunit'])*decimal.Decimal((cstx['result']['percenttoissuer'])/decimal.Decimal(100)))
        dbExecute("insert into addressesintxs "
                  "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                  "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

        if Valid:
          updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)

        #now update with crowdsale specific property details
        Address = rawtx['result']['sendingaddress']
        if getdivisible_MP(PropertyID):
          value=int(decimal.Decimal(rawtx['result']['purchasedtokens'])*decimal.Decimal(1e8))
        else:
          value=int(rawtx['result']['purchasedtokens'])
        value_neg=(value*-1)
        BalanceAvailableCreditDebit=value
 
      #elif type == 52:
        #promote crowdsale does what?

      elif type == 53:
        #Close Crowdsale
        AddressRole = "issuer"
        BalanceAvailableCreditDebit=0
        #update smart property table
        insertProperty(rawtx, Protocol)

      #write output of the address details
      dbExecute("insert into addressesintxs "
                "(Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit)"
                "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (Address, PropertyID, Protocol, TxDBSerialNum, AddressTxIndex, AddressRole, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit))

      if Valid:
        updateBalance(Address, Protocol, PropertyID, Ecosystem, BalanceAvailableCreditDebit, BalanceReservedCreditDebit, BalanceAcceptedCreditDebit, TxHash)


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
    try:
      dbc.execute("select txdbserialnum from transactions where txhash=%s",[txhash])
      ROWS= dbc.fetchall()
      if len(ROWS)==0:
          return -1
      else:
          return ROWS[0][0]
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e
      exit

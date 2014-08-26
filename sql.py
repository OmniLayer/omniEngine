import psycopg2, psycopg2.extras
import csv
import datetime
import decimal
from rpcclient import *

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

dbc=sql_connect()


def select():
    try:
      dbc.execute("select * from transactions")
      ROWS= dbc.fetchall()
      print len(ROWS)
      print ROWS
    except psycopg2.DatabaseError, e:
      print 'Error %s' % e

def resetbalances_MP():
    #for now sync / reset balance data from mastercore balance list
    protocol="Mastercoin"
    #Find all known properties in mastercore
    for property in listproperties_MP()['result']:
      PropertyID = property['propertyid']
      if PropertyID == 2 or ( PropertyID >= 2147483651 and PropertyID <= 4294967295 ):
        Ecosystem= "Test"
      else:
        Ecosystem= "Production"
      bal_data=getallbalancesforid_MP(PropertyID)

      #Check each address and get balance info
      for addr in bal_data['result']:
        Address=addr['address']
        if property['divisible']:
          BalanceAvailable=int(decimal.Decimal(addr['balance'])*decimal.Decimal(1e8))
          BalanceReserved=int(decimal.Decimal(addr['reserved'])*decimal.Decimal(1e8))
        else:
          BalanceAvailable=int(addr['balance'])
          BalanceReserved=int(addr['reserved'])

        try:
          dbc.execute("select address from AddressBalances where address=%s and protocol=%s and propertyid=%s", 
                      (Address, protocol, PropertyID) )
          rows=dbc.fetchall()

          if len(rows) == 0:
            #address not in database, insert
            dbc.execute("INSERT into AddressBalances "
                        "(Address, protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (Address, protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved) )
          else:
            #address in database update
             dbc.execute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s where address=%s and PropertyID=%s", 
                         (BalanceAvailable, BalanceReserved, address, PropertyID) )

          con.commit()
        except psycopg2.DatabaseError, e:
          if con:
            con.rollback()
          print 'Error %s' % e
          sys.exit(1)

def update_balance(Address, Protocol, PropertyID, ecosystem, BalanceAvailable, BalanceReserved, LastTxHash):
    try:
      dbc.execute("select address from AddressBalances where address=%s and protocol=%s and propertyid=%s",
                  (Address, Protocol, PropertyID) )
      rows=dbc.fetchall()

      if len(rows) == 0:
        #address not in database, insert
        dbc.execute("INSERT into AddressBalances "
                    "(Address, protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, LastTxHash) "
                    "VALUES (%s,%s,%s,%s,%s,%s,decode(%s,'hex'))",
                    (Address, protocol, PropertyID, Ecosystem, BalanceAvailable, BalanceReserved, LastTxHash) )
      else:
        #address in database update
        BalanceAvailable=BalanceAvailable+rows['balanceavailable']
        BalanceReserved=BalanceReserved+rows['balancereserved']
        dbc.execute("UPDATE AddressBalances set BalanceAvailable=%s, BalanceReserved=%s, LastTxHash=decode(%s,'hex') where address=%s and PropertyID=%s",
                    (BalanceAvailable, BalanceReserved, LastTxHash, address, PropertyID) )

      con.commit()
    except psycopg2.DatabaseError, e:
      if con:
        con.rollback()
      print 'Error %s' % e
      sys.exit(1)


def insert_tx(rawtx, protocol, blockheight, seq):
    TxHash = rawtx['result']['txid']
    TxBlockTime = datetime.datetime.utcfromtimestamp(rawtx['result']['blocktime'])
    TxErrorCode = rawtx['error']
    TxSeqInBlock= seq

    if protocol == "Bitcoin":
      #Bitcoin is only simple send, type 0
      TxType=0
      TxVersion=rawtx['result']['version']
      TxState= "True"
      Ecosystem= "Production"
      TxSubmitTime = datetime.datetime.utcfromtimestamp(rawtx['result']['time'])

    elif protocol == "Mastercoin":
      #currently type a text output from mastercore 'Simple Send' and version is unknown
      TxType= get_TxType(rawtx['result']['type'])
      TxVersion= 0
      TxState= rawtx['result']['valid']
      #Use block time - 10 minutes to approx
      TxSubmitTime = TxBlockTime-datetime.timedelta(minutes=10)
      if rawtx['result']['propertyid'] == 2 or ( rawtx['result']['propertyid'] >= 2147483651 and rawtx['result']['propertyid'] <= 4294967295 ):
        Ecosystem= "Test"
      else:
        Ecosystem= "Production"

    else:
      print "Wrong protocol? Exiting, goodbye."
      exit(1)

    try:
        dbc.execute("INSERT into transactions "
                    "(TxHash, protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock, TxBlockTime ) "
                    "VALUES (decode(%s,'hex'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                    (TxHash, protocol, TxType, TxVersion, Ecosystem, TxSubmitTime, TxState, TxErrorCode, TxBlockNumber, TxSeqInBlock, TxBlockTime))
        con.commit()
        #need validation on structure
        dbc.execute("Select dbtxserial from transacations where txhash=decode(%s, 'hex')", TxHash)
        serial=dbc.fetchall()['0']['dbtxserial']
        return serial
    except psycopg2.DatabaseError, e:
	if con:
            con.rollback()
	print 'Error %s' % e
        sys.exit(1)

def dumptxaddr_csv(csvwb, rawtx, protocol):
    TxHash = rawtx['result']['txid']

    if protocol == "Bitcoin":
      PropertyID=0
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
            row={'Address': addr, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex, 
                 'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit}
            csvwb.writerow(row)

      #process all inputs, Start AddressTxIndex=0 since inputs don't have a Index number in json and iterate for each input
      AddressTxIndex=0
      for input in rawtx['result']['vin']:
        #check if we have previous input txids we need to lookup or if its a coinbase (newly minted coin ) which needs to be skipped
        if 'txid' in input:
          AddressRole="sender"
          #existing json doesn't have raw address only prev tx. Get prev tx to decipher address/values
          prevtx=getrawtransaction(input['txid'])
          BalanceAvailableCreditDebit=int(decimal.Decimal(prevtx['result']['vout'][input['vout']]['value'])*decimal.Decimal("1e8")*decimal.Decimal(-1))
          #BalanceAvailableCreditDebit=int(prevtx['result']['vout'][input['vout']]['value'] * 1e8 * -1)
          #multisigs have more than 1 address, make sure we find/credit all multisigs for a tx
          for addr in prevtx['result']['vout'][input['vout']]['scriptPubKey']['addresses']:
            row={'Address': addr, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
                 'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit}
            csvwb.writerow(row)
          AddressTxIndex+=1

    elif protocol == "Mastercoin":
      PropertyID= rawtx['result']['propertyid']
      AddressTxIndex=0
      AddressRole="sender"
      type=get_TxType(rawtx['result']['type'])
      Address = rawtx['result']['sendingaddress']
      BalanceAvailableCreditDebit=""
      BalanceResForOfferCreditDebit=""
      BalanceResForAcceptCreditDebit=""

      if rawtx['result']['divisible']:
        value=int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
      else:
        value=int(rawtx['result']['amount'])
      value_neg=(value*-1)

      if type == 0:
        #Simple Send

	#debit the sender
        row={'Address': Address, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
               'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': value_neg}
        csvwb.writerow(row)

	#credit the reciever
        Address = rawtx['result']['referenceaddress']
        BalanceAvailableCreditDebit=value

      #elif type == 2:
	#Restricted Send does nothing yet?

      #elif type == 3:
        #Send To Owners
	#Do something smart

      elif type == 20:
        #DEx Sell Offer
        #Move the amount from Available balance to reserved for Offer
        ##Sell offer cancel doesn't display an amount from core, not sure what we do here yet
        BalanceAvailableCreditDebit = value_neg
        BalanceResForOfferCreditDebit = value

      #elif type == 21:
        #MetaDEx: Offer/Accept one Master Protocol Coins for another

      elif type == 22:
        #DEx Accept Offer
        #Move the amount from Reserved for Offer to Reserved for Accept
        ## Mastercore doesn't show payments as MP tx. How do we credit a user who has payed?
        Address = rawtx['result']['referenceaddress']
        BalanceResForAcceptCreditDebit = value
        BalanceResForOfferCreditDebit = value_neg

      elif type == 50:
        #Fixed Issuance, create property
        AddressRole = "issuer"

      elif type == -51:
        #Participating in crowdsale

        #First deduct the amount the participant sent to 'buyin'
        AddressRole = 'participant'
        BalanceAvailableCreditDebit = value_neg
        row={'Address': Address, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceResForOfferCreditDebit': BalanceResForOfferCreditDebit, 'BalanceResForAcceptCreditDebit': BalanceResForAcceptCreditDebit }
        csvwb.writerow(row)

        #Credit the buy in to the issuer
        AddressRole = 'issuer'
        BalanceAvailableCreditDebit = value
        Address= rawtx['result']['referenceaddress']
        row={'Address': Address, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceResForOfferCreditDebit': BalanceResForOfferCreditDebit, 'BalanceResForAcceptCreditDebit': BalanceResForAcceptCreditDebit }
        csvwb.writerow(row)

        #Now start updating the crowdsale propertyid balance info
        PropertyID = rawtx['result']['purchasedpropertyid']

        #add additional functionalty to check/credit the issue when there is a % bonus to issuer
        cstx = getcrowdsale_MP(PropertyID)
        if cstx['result']['percenttoissuer'] > 0:
          if getdivisible_MP(PropertyID):
            BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(cstx['result']['tokensperunit'])*(decimal.Decimal(cstx['result']['percenttoissuer'])/decimal.Decimal(100))*decimal.Decimal(1e8))
          else:  
            BalanceAvailableCreditDebit = int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(cstx['result']['tokensperunit'])*decimal.Decimal((cstx['result']['percenttoissuer'])/decimal.Decimal(100)))
        row={'Address': Address, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceResForOfferCreditDebit': BalanceResForOfferCreditDebit, 'BalanceResForAcceptCreditDebit': BalanceResForAcceptCreditDebit }
        csvwb.writerow(row)

        #now update with crowdsale specific property details
        Address = rawtx['result']['sendingaddress']
        if getdivisible_MP(PropertyID):
          value=int(decimal.Decimal(rawtx['result']['purchasedtokens'])*decimal.Decimal(1e8))
        else:
          value=int(rawtx['result']['purchasedtokens'])
        value_neg=(value*-1)
        BalanceAvailableCreditDebit=value


      row={'Address': Address, 'PropertyID': PropertyID, 'TxHash': TxHash, 'protocol': protocol, 'AddressTxIndex': AddressTxIndex,
           'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit, 
           'BalanceResForOfferCreditDebit': BalanceResForOfferCreditDebit, 'BalanceResForAcceptCreditDebit': BalanceResForAcceptCreditDebit }
      csvwb.writerow(row)


def dumptx_csv(csvwb, rawtx, protocol, block_height, seq):
    TxHash = rawtx['result']['txid']
    TxBlockTime = datetime.datetime.utcfromtimestamp(rawtx['result']['blocktime'])
    TxErrorCode = rawtx['error']
    TxSeqInBlock= seq

    if protocol == "Bitcoin":
      #Bitcoin is only simple send, type 0
      TxType=0
      TxVersion=0
      TxState= "valid"
      Ecosystem= "Production"
      TxSubmitTime = datetime.datetime.utcfromtimestamp(rawtx['result']['time'])

    elif protocol == "Mastercoin":
      #currently type a text output from mastercore 'Simple Send' and version is unknown
      TxType= get_TxType(rawtx['result']['type'])
      TxVersion= 0
      TxState= rawtx['result']['valid']
      #Use block time - 10 minutes to approx
      TxSubmitTime = TxBlockTime-datetime.timedelta(minutes=10)
      if rawtx['result']['propertyid'] == 2 or ( rawtx['result']['propertyid'] >= 2147483651 and rawtx['result']['propertyid'] <= 4294967295 ):
        Ecosystem= "Test"
      else:
        Ecosystem= "Production"

    else:
      print "Wrong protocol? Exiting, goodbye."
      exit(1)

    row={'TxHash': TxHash, 'protocol': protocol, 'TxType': TxType, 'TxVersion': TxVersion, 'Ecosystem': Ecosystem, 
         'TxSubmitTime': TxSubmitTime, 'TxState': TxState, 'TxErrorCode': TxErrorCode, 'TxBlockNumber': block_height, 
         'TxSeqInBlock': TxSeqInBlock, 'TxBlockTime': TxBlockTime}
    #, 'TxMsg': rawtx}
    csvwb.writerow(row)


def get_TxType(text_type):
    convert={"Simple Send": 0 ,
             "Restricted Send": 2,
             "Send To Owners": 3,
             "Automatic Dispensary":-1,
             "DEx Sell Offer": 20,
             "MetaDEx: Offer/Accept one Master Protocol Coins for another": 21,
             "DEx Accept Offer": 22,
             "Create Property - Fixed": 50,
             "Create Property - Variable": 51,
             "Promote Property": 52,
             "Close Crowsale": 53,
             "Crowdsale Purchase": -51
           }
    return convert[text_type]

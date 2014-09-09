import csv
import datetime
import decimal
import sys
from rpcclient import *
from mscutils import *

def dumpblocks_csv(csvwb, block_data, Protocol, block_height, txcount):
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

    row={'BlockNumber': block_height, 'Protocol': Protocol, 'BlockTime': BlockTime, 'Version': version, 'BlockHash': blockhash,
      'PrevBlock': prevblockhash, 'MerkleRoot': merkleroot, 'Bits': bits, 'Nonce': nonce, 'Size': size,'TxCount': txcount}
    csvwb.writerow(row)

def dumptxaddr_csv(csvwb, rawtx, Protocol, TxDBSerialNum):
    TxHash = rawtx['result']['txid']

    if Protocol == "Bitcoin":
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
            row={'Address': addr, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex, 
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
            row={'Address': addr, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
                 'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit}
            csvwb.writerow(row)
          AddressTxIndex+=1

    elif Protocol == "Mastercoin":
      AddressTxIndex=0
      AddressRole="sender"
      type=get_TxType(rawtx['result']['type'])
      BalanceAvailableCreditDebit=""
      BalanceReservedCreditDebit=""
      BalanceAcceptedCreditDebit=""
      Address = rawtx['result']['sendingaddress']

      #Check if we are a DEx Purchase/payment. Format is a littler different and variables below would fail if we tried. 
      if type != -22:
        PropertyID= rawtx['result']['propertyid']
        if rawtx['result']['divisible']:
          value=int(decimal.Decimal(rawtx['result']['amount'])*decimal.Decimal(1e8))
        else:
          value=int(rawtx['result']['amount'])
        value_neg=(value*-1)

      if type == 0:
        #Simple Send

	#debit the sender
        row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': value_neg, 
             'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit}
        csvwb.writerow(row)

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
        #Move the amount from Reserved for Offer to Reserved for Accept
        ## Mastercore doesn't show payments as MP tx. How do we credit a user who has payed?

        #update the buyer
        AddressRole='buyer'
        BalanceAcceptedCreditDebit = value
        row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit, 
             'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
        csvwb.writerow(row)

        AddressRole='seller'
        Address = rawtx['result']['referenceaddress']
        BalanceAcceptedCreditDebit = value
        BalanceReservedCreditDebit = value_neg

      elif type == -22:
        #DEx Accept Payment

        Sender =  Address
        #process all purchases in the transaction 
        for payment in rawtx['result']['purchases']:

          Receiver = payment['referenceaddress']
          PropertyIDBought = payment['propertyid']
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
          BalanceAvailableCreditDebit=AmountBoughtNeg
          row={'Address': Receiver, 'PropertyID': PropertyIDBought, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
               'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
               'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
          csvwb.writerow(row)

          #Credit tokens to buyer and reduce their accepted amount by amount bought
          AddressRole = 'buyer'
          BalanceAvailableCreditDebit=AmountBought
          BalanceAcceptedCreditDebut=AmountBoughtNeg
          row={'Address': Sender, 'PropertyID': PropertyIDBought, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
               'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
               'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
          csvwb.writerow(row)

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
        #dbc.execute("insert into PropertyHistory (Protocol, PropertyID, TxDBSerialNum) Values(%s, %s, %s)", (Protocol, PropertyID, TxDBSerialNum))

        #First deduct the amount the participant sent to 'buyin'
        AddressRole = 'participant'
        BalanceAvailableCreditDebit = value_neg
        row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
        csvwb.writerow(row)

        #Credit the buy in to the issuer
        AddressRole = 'issuer'
        BalanceAvailableCreditDebit = value
        Address= rawtx['result']['referenceaddress']
        row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
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
        row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
             'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit,
             'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
        csvwb.writerow(row)

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
        #update smart property table
        insertProperty(rawtx, Protocol)


      #write output of the address details
      row={'Address': Address, 'PropertyID': PropertyID, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'AddressTxIndex': AddressTxIndex,
           'AddressRole': AddressRole, 'BalanceAvailableCreditDebit': BalanceAvailableCreditDebit, 
           'BalanceReservedCreditDebit': BalanceReservedCreditDebit, 'BalanceAcceptedCreditDebit': BalanceAcceptedCreditDebit }
      csvwb.writerow(row)


def dumptx_csv(csvwb, rawtx, Protocol, block_height, seq, dbserialnum):
    TxHash = rawtx['result']['txid']
    TxBlockTime = datetime.datetime.utcfromtimestamp(rawtx['result']['blocktime'])
    TxErrorCode = rawtx['error']
    TxSeqInBlock= seq
    TxDBSerialNum = dbserialnum

    if Protocol == "Bitcoin":
      #Bitcoin is only simple send, type 0
      TxType=0
      TxVersion=rawtx['result']['version']
      TxState= "valid"
      Ecosystem= ""
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
      TxSubmitTime=""
      #if rawtx['result']['propertyid'] == 2 or ( rawtx['result']['propertyid'] >= 2147483651 and rawtx['result']['propertyid'] <= 4294967295 ):
      #  Ecosystem= "Test"
      #else:
      #  Ecosystem= "Production"

    else:
      print "Wrong Protocol? Exiting, goodbye."
      exit(1)

    row={'TxHash': TxHash, 'Protocol': Protocol, 'TxDBSerialNum': TxDBSerialNum, 'TxType': TxType, 'TxVersion': TxVersion, 'Ecosystem': Ecosystem, 
         'TxSubmitTime': TxSubmitTime, 'TxState': TxState, 'TxErrorCode': TxErrorCode, 'TxBlockNumber': block_height, 
         'TxSeqInBlock': TxSeqInBlock}
    csvwb.writerow(row)

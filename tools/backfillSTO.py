import csv
from sql import *


def processSTO(Sender, Amount, PropertyID, Protocol, TxDBSerialNum, owners=None):
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
         #dont update balance table, just fill in txs
         #updateBalance(Address, Protocol, PropertyID, Ecosystem, amountSent, None, None, LastHash)
         #make sure we keep track of how much was sent/left to send
         toDistribute-=amountSent
      #/end if amountToSend > 0
      #relative position of the recipiant
      AddressTxIndex+=1
      #no money left to distribute. Done
      if toDistribute == 0:
        break
    #/end for holder in owners


with open('data/owners.data', 'rb') as csvfile:
  fr = csv.reader(csvfile)
  toProcess={}
  skip=False
  for row in fr:
    if row[0] == 'BLOCK':
      block=row[1]
      txid=row[3]
      sender=row[5]
      if 'not' in dbSelect("select txstate from transactions where txhash=%s",[txid])[0][0]:
        skip=True
      else:
        skip=False
        toProcess[txid]={'sender':sender, 'block':block, 'balance':{}}
      print "Found", txid, skip
    elif '#' in row[0]:
      if not skip:
        address=row[2]
        balance=row[1]
        toProcess[txid]['balance'].update({address:balance})

for x in toProcess:

  TxHash = x
  dbData=dbSelect("select atx.txdbserialnum, propertyid, balanceavailablecreditdebit from addressesintxs atx "
                  "inner join transactions tx on (atx.txdbserialnum=tx.txdbserialnum) where tx.txhash=%s",[TxHash])

  Sender = toProcess[x]['sender']
  SentAmount=dbData[0][2]*-1
  PropertyID=dbData[0][1]
  Protocol="Mastercoin"
  TxDBSerialNum=dbData[0][0]

  owners=[]
  #Now dump the updated addressbalance to a list for sendToOwners call to use
  for z in toProcess[x]['balance']:
      temp = [z,decimal.Decimal(toProcess[x]['balance'][z])]
      owners.append(temp)

  print owners
  print Sender, SentAmount, PropertyID, Protocol, TxDBSerialNum

  processSTO(Sender, SentAmount, PropertyID, Protocol, TxDBSerialNum, owners)
  dbCommit()
  raw_input('Updated list, Press <ENTER> to continue')






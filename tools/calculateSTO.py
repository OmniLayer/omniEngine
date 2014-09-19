from sql import *

txs=dbSelect("select tx.txblocknumber,tx.txdbserialnum,atx.address,atx.propertyid,atx.balanceavailablecreditdebit from transactions tx, addressesintxs atx "
             "where tx.txdbserialnum=atx.txdbserialnum and tx.txtype =3 and atx.addressrole='sender' and txstate='valid' order by txdbserialnum")

Protocol="Mastercoin"


#check/calculate the address outputs for each valid tx from sendtoowners
for x in txs:

  TxBlockNum=x[0]
  TxDBSerialNum=x[1]
  Sender=x[2]
  PropertyID=x[3]
  SentAmount=x[4]

  print "Processing:", TxDBSerialNum, "Property:", PropertyID, "Sender:", Sender

  #get the current address balances to walk back from
  addressbalances=dbSelect("select address,balanceavailable,balancereserved,balanceaccepted from addressbalances where propertyid=%s",[PropertyID])
  balance={}
  for x in addressbalances:
    if x[1] == None:
      dbBalanceAvailable = 0
    else:
      dbBalanceAvailable = x[1]
    if x[2] == None:
      dbBalanceReserved = 0
    else:
      dbBalanceReserved = x[2]
    if x[3] == None:
      dbBalanceAccepted = 0
    else:
      dbBalanceAccepted = x[3]
    balance[x[0]]={ 'available': dbBalanceAvailable, 'reserved': dbBalanceReserved, 'accepted': dbBalanceAccepted}

  #get all records from addressintxs that affected our propertyid after the send to owner block was processed
  changedata=dbSelect("select address, balanceavailablecreditdebit, balancereservedcreditdebit, balanceacceptedcreditdebit "
                      "from addressesintxs atx, transactions tx where atx.txdbserialnum=tx.txdbserialnum and "
                      "atx.propertyid=%s and tx.txblocknumber > %s order by atx.txdbserialnum desc", [PropertyID, TxBlockNum])
 
  for bal in changedata:
    #process each addressintxs record to reverse the balances
    Address=bal[0]
    if bal[1] == None:
      available = balance[Address]['available']
    else:
      available = balance[Address]['available']-bal[1]
    if bal[2] == None:
      reserved = balance[Address]['reserved']
    else:
      reserved = balance[Address]['reserved']-bal[2]
    if bal[3] == None:
      accepted = balance[Address]['accepted']
    else:
      accepted = balance[Address]['accepted']-bal[3]
    
    balance[Address]={'available': available, 'reserved': reserved, 'accepted': accepted}

  #Purge the sender from our list for final conversion
  balance.pop(Address, None)

  dictlist=[]
  #Now dump the updated addressbalance to a list for sendToOwners
  for key, value in balance.iteritems():
    if value['available'] > 0:
      temp = [key,value['available']]
      dictlist.append(temp)

  #call sendToOwners and update the output
  sendToOwners(Sender, SentAmount, PropertyID, Protocol, TxDBSerialNum, owners)
  dbCommit()
  raw_input('Updated list, Press <ENTER> to continue')


from sql import *
from csvtools import *

#start db connection 
dbInit()


#Need to change variables back above here if using csv
##appendname=str(initialBlock)+'.'+str(endBlock)
#csv output file info for tx table
##fieldnames = ['TxHash', 'Protocol', 'TxDBSerialNum', 'TxType', 'TxVersion', 'Ecosystem', 'TxSubmitTime', 
##              'TxState', 'TxErrorCode', 'TxBlockNumber', 'TxSeqInBlock']
##out_file = open('data/tx.'+appendname+'.csv', "wb") 
##tx_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
##tx_table.writerow(dict((fn,fn) for fn in fieldnames))

#csv output file info for tx_addr table
##fieldnames = ['Address', 'PropertyID', 'Protocol', 'TxDBSerialNum', 'AddressTxIndex', 'AddressRole', 'BalanceAvailableCreditDebit',
##              'BalanceReservedCreditDebit', 'BalanceAcceptedCreditDebit']
##out_file = open('data/txaddr.'+appendname+'.csv', "wb")
##txaddr_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
##txaddr_table.writerow(dict((fn,fn) for fn in fieldnames))

#csv output file info for blocks table
##fieldnames = ['BlockNumber', 'Protocol', 'BlockTime', 'Version', 'BlockHash',
##      'PrevBlock', 'MerkleRoot', 'Bits', 'Nonce', 'Size','TxCount']
##out_file = open('data/blocks.'+appendname+'.csv', "wb")
##blocks_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
##blocks_table.writerow(dict((fn,fn) for fn in fieldnames))

#There are 2 bitcoin transactions that have duplicate hashes in different blocks. 
#We skip them here to avoid database issues
#We don't need to skip them, the index will tolerate them now.
#skip={'d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599': 91842,
#      'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468': 91880}

#block with first MP transaction
firstMPtxBlock=249948

#get last known block processed from db
initialBlock=dbSelect("select max(blocknumber) from blocks", None)[0][0]+1

#initialBlock=firstMPtxBlock
#initialBlock=253056

endBlock=getinfo()['result']['blocks']
#endBlock=253060

#get highest TxDBSerialNum (number of rows in the Transactions table)
TxDBSerialNum=dbSelect('select last_value from transactions_txdbserialnum_seq',None)[0][0]+1

#Start at 1 since block 0 is special case
#21479844 btc tx's before block 249948
#TxDBSerialNum=21479844

currentBlock=initialBlock


#main loop, process new blocks
while currentBlock <= endBlock:
 try:
  hash = getblockhash(currentBlock)['result']
  block_data = getblock(hash)
  height = block_data['result']['height']

  #expire the current active offers if block time has passed
  expireAccepts(height)

  #don't waste resources looking for MP transactions before the first one occurred
  if height >= firstMPtxBlock:
    block_data_MP = listblocktransactions_MP(height)
  else:
    block_data_MP = {"error": None, "id": None, "result": []}

  #Status update every 10 blocks
  if height % 10 == 0 or currentBlock == initialBlock:
    print "Block", height, "of", endBlock

  #Process Bitcoin Transacations
  Protocol="Bitcoin"
  #Prime tx sequence number based on number of tx
  x=len(block_data['result']['tx'])
  print  x, "BTC tx"

  #Write the blocks table row
  #dumpblocks_csv(blocks_table, block_data, Protocol, height, x)
  insertBlock(block_data, Protocol, height, x)

  for tx in block_data['result']['tx']:
    #rawtx=getrawtransaction(tx)
    #add the transaction and addresses in the transaction to the csv files
    #dumptx_csv(tx_table, rawtx, Protocol, height, x, TxDBSerialNum)
    #dumptxaddr_csv(txaddr_table, rawtx, Protocol, TxDBSerialNum)

    #manualtxnum=TxDBSerialNum
    #manualtxnum=-1
    #serial=insertTx(rawtx, Protocol, height, x, TxDBSerialNum)
    #insertTxAddr(rawtx, Protocol, serial, currentBlock)

    #increment the number of transactions
    TxDBSerialNum+=1
    #decrement tx sequence number in block
    x-=1


  #Process Mastercoin Transacations (if any)
  Protocol="Mastercoin"
  #prime tx sequence number based on number of msc tx
  x=len(block_data_MP['result'])
  if x != 0:
    print  x, "MSC tx"

  for tx in block_data_MP['result']:
    rawtx=gettransaction_MP(tx)
    #add the transaction and addresses in the transaction to the csv files
    #dumptx_csv(tx_table, rawtx, Protocol, height, x, TxDBSerialNum)
    #dumptxaddr_csv(txaddr_table, rawtx, Protocol, TxDBSerialNum)
  
    #use -1 to auto assign TxDBSerialNum otherwise specify it here
    manualtxnum=TxDBSerialNum
    #manualtxnum=-1
    serial=insertTx(rawtx, Protocol, height, x, manualtxnum)
    insertTxAddr(rawtx, Protocol, serial, currentBlock)

    #increment the number of transactions
    TxDBSerialNum+=1
    #decrement tx sequence number in block
    x-=1    

  #make sure we store the last serialnumber used
  dbExecute("select setval('transactions_txdbserialnum_seq', %s)", [TxDBSerialNum-1])
  #write db changes for entire block
  dbCommit()

 except Exception,e:
  print "Problem with ", e
  if dbRollback():
    print "Database rolledback, last successful block", (currentBlock -1)
  else:
    print "Problem rolling database back, check block data for", currentBlock
  exit(1)

 #increment to next block
 currentBlock += 1

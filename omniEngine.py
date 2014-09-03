#from rpcclient import *
from sql import *

dbc=sql_connect()

#block with first MP transaction
firstMPtxBlock=249948

#get last known block from the RPC client
initialBlock=0
#endBlock=getinfo()['result']['blocks']
endBlock=99999

#get highest TxDBSerialNum (number of rows in the Transactions table)
TxDBSerialNum=-1

appendname=str(initialBlock)+'.'+str(endBlock)
#csv output file info for tx table
fieldnames = ['TxHash', 'Protocol', 'TxDBSerialNum', 'TxType', 'TxVersion', 'Ecosystem', 'TxSubmitTime', 
              'TxState', 'TxErrorCode', 'TxBlockNumber', 'TxSeqInBlock']
out_file = open('data/tx.'+appendname+'.csv', "wb") 
tx_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
tx_table.writerow(dict((fn,fn) for fn in fieldnames))

#csv output file info for tx_addr table
fieldnames = ['Address', 'PropertyID', 'Protocol', 'TxDBSerialNum', 'AddressTxIndex', 'AddressRole', 'BalanceAvailableCreditDebit',
              'BalanceResForOfferCreditDebit', 'BalanceResForAcceptCreditDebit']
out_file = open('data/txaddr.'+appendname+'.csv', "wb")
txaddr_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
txaddr_table.writerow(dict((fn,fn) for fn in fieldnames))

#csv output file info for blocks table
fieldnames = ['BlockNumber', 'Protocol', 'BlockTime', 'Version', 'BlockHash',
      'PrevBlock', 'MerkleRoot', 'Bits', 'Nonce', 'Size','TxCount']
out_file = open('data/blocks.'+appendname+'.csv', "wb")
blocks_table = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
blocks_table.writerow(dict((fn,fn) for fn in fieldnames))

#There are 2 bitcoin transactions that have duplicate hashes in different blocks. 
#We skip them here to avoid database issues
#We don't need to skip them, the index will tolerate them now.
#skip={'d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599': 91842,
#      'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468': 91880}


currentBlock=initialBlock;

#main loop
while currentBlock <= endBlock:
 try:
  #address_data=host.call("getallbalancesforaddress_MP", addr)
  hash = getblockhash(currentBlock)['result']
  block_data = getblock(hash)
  #print json.dumps(block_data, indent=2, sort_keys=True)
  height = block_data['result']['height']
  #don't look for MP transactions before the first one occurred
  if height >= firstMPtxBlock:
    block_data_MP = listblocktransactions_MP(height)
    #print json.dumps(block_data_MP, indent=2, sort_keys=True)

  if height % 10 == 0 or currentBlock == initialBlock:
    print "Block", height, "of", endBlock

  #prime tx sequence number based on number of tx
  x=len(block_data['result']['tx'])
  print  x, "BTC tx"
  #don't look for MP transactions before the first one occurred
  if height >= firstMPtxBlock:
    mx=len(block_data_MP['result'])
    print  mx, "MP tx"
  else:
    mx=0

  #write the blocks table row
  dumpblocks_csv(blocks_table, block_data, "Bitcoin", height, x)
  for tx in block_data['result']['tx']:
    #print 'tx:', tx
    #print 'block_data_MP:', block_data_MP
    #if this is a Mastercoin tx, say so
    if mx > 0 and tx in block_data_MP['result']:
      rawtx=gettransaction_MP(tx)
      #print 'MP:', rawtx
      Protocol="Mastercoin"
    else:
      rawtx=getrawtransaction(tx)
      #print 'BTC:', rawtx
      Protocol="Bitcoin"
    #don't need to skip these 2 transactions any more
    #if tx in skip and skip[tx] == height:
      #print "Skipping bad tx"
    #else:

    #increment the number of transactions
    TxDBSerialNum+=1;
    #add the transaction and addresses in the transaction to the csv files
    dumptx_csv(tx_table, rawtx, Protocol, height, x, TxDBSerialNum)
    dumptxaddr_csv(txaddr_table, rawtx, Protocol, TxDBSerialNum)
    # insert_transaction(dbc, rawtx, "Bitcoin", height)
    #decrement tx sequence number in block
    x-=1

  #prime tx sequence number based on number of tx
  #x=len(block_data_MP['result'])
  #print "Found ", x, "Mastercoin transactions"
  #if x != 0:
    #print  x, "MSC tx"
  #for tx in block_data_MP['result']:
    #rawtx=gettransaction_MP(tx)
    #insert_transaction(dbc, rawtx, "Mastercoin", height)
    #dumptx_csv(tx_table, rawtx, "Mastercoin", height, x)
    #dumptxaddr_csv(txaddr_table, rawtx, "Mastercoin")
    #decrement tx sequence number in block
    #x-=1

  #rawtx=host.call("getrawtransaction", tx, 1)
  #print json.dumps(block_data, indent=2, sort_keys=True)
  #print json.dumps(rawtx, indent=2, sort_keys=True)
  #dump_csv(writer, rawtx, "Bitcoin", height)


 except Exception,e:
  print "Problem with ", e

 #increment to next block
 currentBlock += 1


#print json.dumps(block_data,indent=2)
#print address_data


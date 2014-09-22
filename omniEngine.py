from sql import *
import os.path
from datetime import datetime

lockFile='/tmp/omniEngine.lock'
now=datetime.now()

if os.path.isfile(lockFile):
  #open the lock file to read pid and timestamp
  file=open(lockFile,'r')
  pid=file.readline().replace("\n", "")
  timestamp=file.readline()
  file.close()
  #check if the pid is still running
  if os.path.exists("/proc/"+str(pid)):
    print "Exiting: OmniEngine already running with pid:", pid, "  Last parse started at ", timestamp
  else:
    print "Stale OmniEngine found, no running pid:", pid, " Process last started at: ", timestamp
    print "Removing lock file and waiting for restart"
    os.remove(lockFile)
  #exit program and wait for next run
  exit(1)
else:
  #start/create our lock file
  file = open(lockFile, "w")
  file.write(str(os.getpid()))
  file.write(str(now))
  file.close() 

  print "Processing started at ", now

  #block with first MP transaction
  firstMPtxBlock=249948

  #get last known block processed from db
  currentBlock=dbSelect("select max(blocknumber) from blocks", None)[0][0]+1

  #Find most recent block mastercore has available
  endBlock=getinfo()['result']['blocks']

  if currentBlock > endBlock:
    print "Already up to date"

  #get highest TxDBSerialNum (number of rows in the Transactions table)
  TxDBSerialNum=dbSelect('select last_value from transactions_txdbserialnum_seq',None)[0][0]+1
  #21479844 btc tx's before block 249948
  #TxDBSerialNum=21479844

  #main loop, process new blocks
  while currentBlock <= endBlock:
    try:
      hash = getblockhash(currentBlock)['result']
      block_data = getblock(hash)
      height = block_data['result']['height']

      #don't waste resources looking for MP transactions before the first one occurred
      if height >= firstMPtxBlock:
        block_data_MP = listblocktransactions_MP(height)
      else:
        block_data_MP = {"error": None, "id": None, "result": []}

      #Status update every 10 blocks
      if height % 10 == 0 or currentBlock:
        print "Block", height, "of", endBlock

      #Process Bitcoin Transacations
      Protocol="Bitcoin"

      #Find number of tx's in block
      txcount=len(block_data['result']['tx'])
      print  txcount, "BTC tx"

      #Write the blocks table row
      insertBlock(block_data, Protocol, height, txcount)

      #count position in block
      x=1
      for tx in block_data['result']['tx']:
        #rawtx=getrawtransaction(tx)
        #serial=insertTx(rawtx, Protocol, height, x, TxDBSerialNum)
        #serial=insertTx(rawtx, Protocol, height, x)
        #insertTxAddr(rawtx, Protocol, serial, currentBlock)

        #increment the number of transactions
        TxDBSerialNum+=1
        #increment tx sequence number in block
        x+=1

      #Process Mastercoin Transacations (if any)
      Protocol="Mastercoin"

      #Find number of msc tx
      y=len(block_data_MP['result'])
      if y != 0:
        print  y, "MSC tx"

      #count position in block
      x=1
      #MP tx processing
      for tx in block_data_MP['result']:
        rawtx=gettransaction_MP(tx)
  
        #Process the bare tx and insert it into the db
        #TxDBSerialNum can be specified for explit insert or left out to auto assign from next value in db
        serial=insertTx(rawtx, Protocol, height, x, TxDBSerialNum)
        #serial=insertTx(rawtx, Protocol, height, x)

        #Process all the addresses in the tx and insert them into db
        #This also calls the functions that update the DEx, SmartProperty and AddressBalance tables
        insertTxAddr(rawtx, Protocol, serial, currentBlock)

        #increment the number of transactions
        TxDBSerialNum+=1

        #increment tx sequence number in block
        x+=1    

      #Clean up any offers/crowdsales that expired in this block
      #Run these after we processes the tx's in the block as tx in the current block would be valid
      #expire the current active offers if block time has passed
      expireAccepts(height)
      #check any active crowdsales and update json if the endtime has passed (based on block time)
      expireCrowdsales(block_data['result']['time'], Protocol)
      #exodus address generates dev msc, sync our balance to match the generated balanace
      syncAddress('1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4P', Protocol)

      #make sure we store the last serialnumber used
      dbExecute("select setval('transactions_txdbserialnum_seq', %s)", [TxDBSerialNum-1])
      #write db changes for entire block
      dbCommit()

    except Exception,e:
      #Catch any issues and stop processing. Try to undo any incomplete changes
      print "Problem with ", e
      if dbRollback():
        print "Database rolledback, last successful block", (currentBlock -1)
      else:
        print "Problem rolling database back, check block data for", currentBlock
      os.remove(lockFile)
      exit(1)

    #increment/process next block if everything went clean
    currentBlock += 1

  #/while loop.  Finished processing all current blocks. 
  #remove the lock file and let ourself finish
  os.remove(lockFile)

#/end else for lock file

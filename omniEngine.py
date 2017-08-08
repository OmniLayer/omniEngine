from sql import *
import os.path
import sys
from datetime import datetime
from datetime import timedelta
from cacher import *
import config

USER=getpass.getuser()
lockFile='/tmp/omniEngine.lock.'+str(USER)
now=datetime.now()
sys.argv.pop(0)
lastStatusUpdateTime=None

if os.path.isfile(lockFile):
  #open the lock file to read pid and timestamp
  file=open(lockFile,'r')
  data=file.readline()
  file.close()
  pid=data.split(',')[0]
  timestamp=data.split(',')[1]
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
  file.write(str(os.getpid())+','+str(now))
  file.close() 

  #set our debug level, all outputs will be controlled by this
  try:
    if len(sys.argv) == 1:
      #use debug level from cmd line
      debuglevel = int(sys.argv[0])
    else:
      #invlid cmdline options use default value
      debuglevel = 5
  except:
    #invlid cmdline options use default value
    debuglevel = 5

  setdebug(debuglevel)

  printdebug(("Processing started at",now), 0)

  #block with first MP transaction
  firstMPtxBlock=252317

  #get last known block processed from db
  currentBlock=dbSelect("select max(blocknumber) from blocks", None)[0][0]
  printdebug(("Current block is ",currentBlock), 0)
  if currentBlock is not None:
    currentBlock=currentBlock+1
  else:
    currentBlock=firstMPtxBlock

  #Find most recent block mastercore has available
  endBlock=getinfo()['result']['blocks']

  #reorg protection/check go back 10 blocks from where we last parsed
  checkBlock=max(currentBlock-10,firstMPtxBlock)
  while checkBlock < currentBlock:
    hash = getblockhash(checkBlock)['result']
    dbhash=dbSelect('select blockhash from blocks where blocknumber=%s',[checkBlock])[0][0]
    if hash == dbhash:
      #everything looks good, go to next block
      checkBlock+=1
    else:
      #reorg took place
      try:
        print "Reorg detected, Attempting roll back to ",checkBlock-1
        reorgRollback(checkBlock-1)
        currentBlock=checkBlock
        dbCommit()
        break
      except Exception,e:
        #Catch any issues and stop processing. Try to undo any incomplete changes
        print "Problem with ", e
        if dbRollback():
          print "Database rolledback, last successful block", (currentBlock -1)
        else:
          print "Problem rolling database back, check block data for", currentBlock
        exit(1)


  if currentBlock > endBlock:
    printdebug("Already up to date",0)
  else:
    rExpireAllBalBTC()

  #get highest TxDBSerialNum (number of rows in the Transactions table)
  #22111443 btc tx's before block 252317
  TxDBSerialNum=dbSelect('select coalesce(max(txdbserialnum), 22111443) from transactions')[0][0]+1

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
        if lastStatusUpdateTime == None:
          printdebug(("Block",height,"of",endBlock),1)
          lastStatusUpdateTime=datetime.now()
        else:
          statusUpdateTime=datetime.now()
          timeDelta = statusUpdateTime - lastStatusUpdateTime
          blocksLeft = endBlock - currentBlock
          projectedTime = str(timedelta(microseconds=timeDelta.microseconds * blocksLeft))
          printdebug(("Block",height,"of",endBlock, "(took", timeDelta.microseconds, "microseconds, blocks left:", blocksLeft, ", eta", projectedTime,")"),1)
          lastStatusUpdateTime=statusUpdateTime

      #Process Bitcoin Transacations
      Protocol="Bitcoin"

      #Find number of tx's in block
      txcount=len(block_data['result']['tx'])
      printdebug((txcount,"BTC tx"), 1)

      #Write the blocks table row
      insertBlock(block_data, Protocol, height, txcount)

      #check for pendingtx's to cleanup
      checkPending(block_data['result']['tx'])

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
      Protocol="Omni"

      #Find number of msc tx
      y=len(block_data_MP['result'])
      if y != 0:
        printdebug((y,"OMNI tx"), 1)

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
      if config.TESTNET:
        syncAddress('mpexoDuSkGGqvqrkrjiFng38QPkJQVFyqv', Protocol)
        #upadate temp orderbook
        #updateorderblob()
      else:
        syncAddress('1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4P', Protocol)

      #Also make sure we update the json data in SmartProperties table used by exchange view
      updateProperty(1,"Omni")
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

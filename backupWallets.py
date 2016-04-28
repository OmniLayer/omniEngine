import os.path
from datetime import datetime
from sqltools import *
from common import *


def backupWallets():
  ROWS=dbSelect("select walletid, walletblob from wallets")
  if len(ROWS) == 0:
    print "No Wallets found to backup"
  else:
    directory=os.path.expanduser("~")+'/walletBackups/'

    if not os.path.exists(directory):
      os.makedirs(directory)

    for wallet in ROWS:
      walletid=wallet[0]
      blob=wallet[1]
      walletfile=str(directory)+str(walletid)+'.json'
      file = open(walletfile, "w")
      file.write(str(blob))
      file.close()

    print str(datetime.now())+" Backed up "+str(len(ROWS))+" wallets to "+str(directory)
 


def main():
  lockFile='/tmp/backupWallets.lock'
  now=datetime.now()

  if os.path.isfile(lockFile):
    #open the lock file to read pid and timestamp
    file=open(lockFile,'r')
    #pid=file.readline().replace("\n", "")
    #timestamp=file.readline()
    x=file.readline()
    file.close()
    pid=x.split(",")[0]
    timestamp=x.split(",")[1]
    #check if the pid is still running
    if os.path.exists("/proc/"+str(pid)):
      print "Exiting: backupWallet already running with pid:", pid, "  Last update started at ", timestamp
    else:
      print "Stale backupWallet found, no running pid:", pid, " Process last started at: ", timestamp
      print "Removing lock file and waiting for restart"
      os.remove(lockFile)
    #exit program and wait for next run
    exit(1)
  else:
    #start/create our lock file
    file = open(lockFile, "w")
    #file.write(str(os.getpid()))
    #file.write(str(now))
    file.write(str(os.getpid())+","+str(now))
    file.close()

    #set our debug level, all outputs will be controlled by this
    setdebug(9)

    try:
      backupWallets()
    except Exception, e:
      #Catch any issues and stop processing. Try to undo any incomplete changes
      print "backupWallets: Problem with ", e
      os.remove(lockFile)
      exit(1)

  #remove the lock file and let ourself finish
  os.remove(lockFile)




if __name__ == "__main__":main() ## with if

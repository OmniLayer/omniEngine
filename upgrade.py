from sqltools import *
import sys, json

sys.argv.pop(0)

print "Please make sure you have backed up your database before proceeding"
username=raw_input('Enter DB admin username: ')
password=raw_input('Enter DB admin password: ')

try:
  if len(sys.argv) == 1:
    try:
      upgradeFile=sys.argv[0]
      with open(upgradeFile) as fp:
        for line in fp:
          cmd=line.strip('\n')
          dbUpgradeExecute(username,password,cmd)
      dbCommit()
      print "Patches Applied Successfully"

    except Exception,e:
      dbRollback()
      print str(e)+" error upgrading, rollingback database changes"

  else:
    print "Usage Guidelines: python upgrade.py <patchfile>"
except Exception,e:
  dbRollback()
  print "Something failed trying to upgrade "+str(e)


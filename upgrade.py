from sqltools import *
import sys, json

sys.argv.pop(0)

try:
  if len(sys.argv) == 1:
    try:
      upgradeFile=sys.argv[0]
      with open(upgradeFile) as fp:
        for line in fp:
          cmd=line.strip('\n')
          dbExecute(cmd)
      dbCommit()
      print "Patches Applied Successfully"

    except e:
      dbRollback()
      print e+" error upgrading, rollingback database changes"

  else:
    print "Usage Guidelines: python upgrade.py <patchfile>"
except Exception,e:
  print "Something failed trying to upgrade "+ee


from sql import *

try:
  #Try to update consensushash of last block
  updateConsensusHash()
  dbCommit()
except:
  pass

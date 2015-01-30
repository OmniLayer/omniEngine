import sys, json
from rpcclient import *

host=RPCHost()

sys.argv.pop(0)

try:
  if len(sys.argv) > 2:
    rpccall= sys.argv.pop(0)
    try:
      data=host.call(rpccall, sys.argv[0], int(sys.argv[1]))
    except:
      data=host.call(rpccall, sys.argv[0], str(sys.argv[1]))
  elif len(sys.argv) == 2:
    if sys.argv[1].isdigit():
      data=host.call(sys.argv.pop(0), int(sys.argv[0]))
    else:
      data=host.call(sys.argv.pop(0), sys.argv[0])
  else:
    data=host.call(sys.argv.pop(0))
  print json.dumps(data,indent=2,sort_keys=True)
except Exception,e:
  print e


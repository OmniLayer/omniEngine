#default debug
debug=0

def setdebug(level):
  global debug
  debug=level

def printdebug(msg,verbose):
  if debug >= verbose:
    if type(msg) == tuple:
      temp=""
      for x in msg:
        temp+=str(x)+" "
      msg=temp
    print str(msg)

from common import *
from rpcclient import getrawtransaction

def getDivisible(rawtx):
  try:
    divisible=rawtx['result']['divisible']
  except KeyError:
    if rawtx['result']['propertytype'] == 'indivisible':
      divisible=False
    else:
      divisible=True
  return divisible  

def getEcosystem(propertyid):
    if propertyid == 2 or ( propertyid >= 2147483651 and propertyid <= 4294967295 ) or propertyid == "test":
       return "Test"
    elif propertyid == 1 or ( propertyid >= 3 and propertyid <= 2147483650) or propertyid == "main":
       return "Production"
    elif propertyid == "all":
      return "All"
    else:
       return None

def getTxState(valid):
    if valid:
      return "valid"
    else:
      return "not valid"
    #there is also pending, but omniEngine won't write that

def getTxClass(txid):
    #Class A = 1 , exodus_marker + no opreturn/no multisig
    #Class B = 2 , exodus marker + multisig
    #Class C = 3 , opreturn
    exodus_array = ['1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4P','mpexoDuSkGGqvqrkrjiFng38QPkJQVFyqv']
    exodus = False
    opreturn = False
    multisig = False
    try:
      rawtx = getrawtransaction(txid)
      for output in rawtx['result']['vout']:
        data = output['scriptPubKey']
        type = data['type']
        if type == 'nulldata':
          opreturn = True
        elif type == 'multisig':
          multisig = True
        else:
          for addr in data['addresses']:
            if addr in exodus_array:
              exodus = True
      if opreturn:
        return 3
      elif exodus and multisig:
        return 2
      elif exodus:
        return 1
      else:
        return 0
    except Exception as e:
      printdebug(('DEBUG: error determining transaction class:', e),4)
      return 0

def get_TxType(text_type):
  try:
    convert={"Simple Send": 0 ,
             "Restricted Send": 2,
             "Send To Owners": 3,
             "Send All": 4,
             "Savings": -1,
             "Savings COMPROMISED": -1,
             "Rate-Limiting": -1,
             "Automatic Dispensary":-1,
             "DEx Sell Offer": 20,
             "MetaDEx: Offer/Accept one Master Protocol Coins for another": 21,
             "MetaDEx: Offer/Accept one Master Protocol Tokens for another": 21,
             "MetaDEx token trade": 21,
             "DEx Accept Offer": 22,
             "DEx Purchase": -22,
             "MetaDEx trade": 25,
             "MetaDEx cancel-price": 26,
             "MetaDEx cancel-pair": 27,
             "MetaDEx cancel-ecosystem": 28,
             "Create Property - Fixed": 50,
             "Create Property - Variable": 51,
             "Crowdsale Purchase": -51,
             "Promote Property": 52,
             "Close Crowdsale": 53,
             "Create Property - Manual": 54,
             "Grant Property Tokens": 55,
             "Revoke Property Tokens": 56,
             "Change Issuer Address": 70,
             "Freeze Property Tokens": 185,
             "Unfreeze Property Tokens": 186,
             "Notification": -1,
             "Feature Activation": 65534,
             "ALERT": 65535
           }
    return convert[text_type]
  except KeyError:
    return -1

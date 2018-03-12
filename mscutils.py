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
    else:
       return None

def getTxState(valid):
    if valid:
      return "valid"
    else:
      return "not valid"
    #there is also pending, but omniEngine won't write that

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

def getEcosystem(propertyid):
    if propertyid == 2 or ( propertyid >= 2147483651 and propertyid <= 4294967295 ):
       return "Test"
    elif propertyid == 1 or ( propertyid >= 3 and propertyid <= 2147483650):
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
             "Automatic Dispensary":-1,
             "DEx Sell Offer": 20,
             "MetaDEx: Offer/Accept one Master Protocol Coins for another": 21,
             "DEx Accept Offer": 22,
             "DEx Purchase": -22,
             "Create Property - Fixed": 50,
             "Create Property - Variable": 51,
             "Promote Property": 52,
             "Close Crowdsale": 53,
             "Crowdsale Purchase": -51
           }
    return convert[text_type]
  except KeyError:
    return -1

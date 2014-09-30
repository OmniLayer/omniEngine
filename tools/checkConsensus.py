from sql import *

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
       return float(obj)
    raise TypeError

x=checkbalances_MP()
print json.dumps(x, indent=2, default=decimal_default)


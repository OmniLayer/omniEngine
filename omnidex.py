import json 
import re
import time
from sqltools import * 
from math import ceil

def fixDecimal(value):
    try:
      return str(ceil(float(value)*(1e8))/1e8)
    except Exception as e:
      print "couldn't convert ",value,"got error: ",e

#@app.route('/book')
def getOrderbook(lasttrade=0, lastpending=0):
    #use for websocket to load/broadcast updated book
    book={}
    trade=0
    updated=False

    #find last know DEx2.0 trade and see if it's newer than what we have
    trades=dbSelect("select max(txdbserialnum) from transactions where txtype >24 and txtype<29 and txstate='valid'")
    if len(trades) > 0 and len(trades[0]) > 0:
      trade=int(trades[0][0])

    pending=dbSelect("select coalesce(min(txdbserialnum),0) from transactions where txtype >24 and txtype<29 and txstate='pending'")
    if len(pending) > 0 and len(pending[0]) > 0:
      pending=int(pending[0][0])

    if (trade > int(lasttrade) or pending < int(lastpending)):
      AO=dbSelect("select distinct propertyiddesired, propertyidselling from activeoffers "
                  "where offerstate='active' order by propertyiddesired")
      if len(AO) > 0:
        for pair in AO:
          pd=int(pair[0])
          ps=int(pair[1])
          if 0 in [pd,ps]:
            #skip dex 1.0 sales
            continue
          data = get_orders_by_market(pd,ps)
          data2 = get_orders_by_market(ps,pd)
          try:
            book[pd][ps]=data
          except KeyError:
            book[pd]={ps: data}
          try:
            book[ps][pd]=data2
          except KeyError:
            book[ps]={pd: data2}
        updated=True

    ret={"updated":updated ,"book":book, "lasttrade":trade, "lastpending":pending}
    return ret
   

def get_orders_by_market(propertyid_desired, propertyid_selling):
    orderbook = dbSelect("SELECT ao.propertyiddesired, ao.propertyidselling, ao.AmountAvailable, ao.AmountDesired, ao.TotalSelling, ao.AmountAccepted, "
                         "cast(txj.txdata->>'unitprice' as numeric), ao.Seller, tx.TxRecvTime, 'active', tx.txhash from activeoffers ao, transactions tx, txjson txj "
                         "where ao.CreateTxDBSerialNum = txj.TxDBSerialNum and ao.CreateTxDBSerialNum = tx.TxDBSerialNum and ao.propertyiddesired = %s and "
                         "ao.propertyidselling = %s and ao.OfferState = 'active' union all select cast(txj.txdata->>'propertyiddesired' as bigint), "
                         "cast(txj.txdata->>'propertyidforsale' as bigint),CASE WHEN txj.txdata->>'propertyidforsaleisdivisible' = 'true' THEN "
                         "round(cast(txj.txdata->>'amountforsale' as numeric) * 100000000) ELSE cast(txj.txdata->>'amountforsale' as numeric) END, "
                         "CASE WHEN txj.txdata->>'propertyiddesiredisdivisible' = 'true' THEN round(cast(txj.txdata->>'amountdesired' as numeric) * 100000000) "
                         "ELSE cast(txj.txdata->>'amountdesired' as numeric) END,CASE WHEN txj.txdata->>'propertyidforsaleisdivisible' = 'true' THEN "
                         "round(cast(txj.txdata->>'amountforsale' as numeric) * 100000000) ELSE cast(txj.txdata->>'amountforsale' as numeric) END,0, "
                         "cast(txj.txdata->>'unitprice' as numeric),txj.txdata->>'sendingaddress', tx.TxRecvTime, 'pending',tx.txhash from transactions tx inner join txjson txj "
                         "on tx.txdbserialnum = txj.txdbserialnum where tx.txdbserialnum < 0 and tx.txtype = 25 and cast(txj.txdata->>'propertyidforsale' as numeric) = %s "
                         "and cast(txj.txdata->>'propertyiddesired' as numeric) = %s",[propertyid_desired,propertyid_selling,propertyid_selling,propertyid_desired])

    cancels = dbSelect("SELECT cast(txj.txdata->>'propertyiddesired' as bigint),cast(txj.txdata->>'propertyidforsale' as bigint),CASE WHEN "
                       "txj.txdata->>'propertyiddesiredisdivisible' = 'true' THEN round(cast(txj.txdata->>'amountdesired' as numeric) * 100000000) "
                       "ELSE cast(txj.txdata->>'amountdesired' as numeric) END,CASE WHEN txj.txdata->>'propertyidforsaleisdivisible' = 'true' THEN "
                       "round(cast(txj.txdata->>'amountforsale' as numeric) * 100000000) ELSE cast(txj.txdata->>'amountforsale' as numeric) END, "
                       "cast(txj.txdata->>'unitprice' as numeric),txj.txdata->>'sendingaddress', tx.TxRecvTime, 'pending', tx.txhash from transactions tx "
                       "inner join txjson txj on tx.txdbserialnum = txj.txdbserialnum where tx.txdbserialnum < 0 and tx.txtype = 26 and "
                       "cast(txj.txdata->>'propertyidforsale' as numeric) = %s and cast(txj.txdata->>'propertyiddesired' as numeric) = %s",
                       [propertyid_selling,propertyid_desired])

    return {"status" : 200, "orderbook": [
        {
            "propertyid_desired":order[0], 
            "propertyid_selling":order[1],
            "available_amount" : str(order[2]),
            "desired_amount" : str(order[3]),
            "total_amount" : str(order[4]),
            "accepted_amount": str(order[5]),
            "unit_price" : fixDecimal(order[6]),
            "seller" : str(order[7]),
            "time" : str(order[8]),
            "status" : order[9],
            "txhash" : str(order[10])
        } for order in orderbook], "cancels":[
        {
            "propertyid_desired":cancel[0], 
            "propertyid_selling":cancel[1],
            "desired_amount" : str(cancel[2]),
            "total_amount" : str(cancel[3]),
            "unit_price" : str(cancel[4]),
            "seller" : str(cancel[5]),
            "time" : str(cancel[6]),
            "txhash" : str(order[7])
        } for cancel in cancels]}

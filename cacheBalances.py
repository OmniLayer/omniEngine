from balancehelper import *
import redis, json, time, datetime

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def printmsg(msg):
    print str(datetime.datetime.now())+str(" ")+str(msg)
    sys.stdout.flush()

def updateBalancesCache():
  while True:
    printmsg("Checking for balance updates")
    try:
      addresses=r.get("omniwallet:balances:addresses")
      if addresses != None:
        addresses = json.loads(addresses)
        printmsg("loaded "+str(len(addresses))+" addresses from redis")
        balances=get_bulkbalancedata(addresses)
        r.set("omniwallet:balances:balbook",json.dumps(balances))
        #expire balance data after 10 minutes (prevent stale data in case we crash)
        r.expire("omniwallet:balances:balbook",600)

    except Exception as e:
      printmsg("error updating balances: "+str(e))
    time.sleep(30)


def main():
  updateBalancesCache()


if __name__ == "__main__":main() ## with if


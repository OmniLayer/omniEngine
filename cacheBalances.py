from balancehelper import *
import redis, json, time, datetime

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def printmsg(msg):
    print str(datetime.datetime.now())+str(" ")+str(msg)
    sys.stdout.flush()

def updateBalancesCache():
  while True:
    printmsg("Checking for balance updates")
    for space in r.keys("omniwallet:balances:addresses*"):
      try:
        addresses=r.get(space)
        if addresses != None:
          addresses = json.loads(addresses)
          printmsg("loaded "+str(len(addresses))+" addresses from redis "+str(space))
          balances=get_bulkbalancedata(addresses)
          r.set("omniwallet:balances:balbook"+str(space[29:]),json.dumps(balances))
          #expire balance data after 10 minutes (prevent stale data in case we crash)
          r.expire("omniwallet:balances:balbook"+str(space[29:]),600)

      except Exception as e:
        printmsg("error updating balances: "+str(space)+' '+str(e))
    time.sleep(30)


def main():
  updateBalancesCache()


if __name__ == "__main__":main() ## with if


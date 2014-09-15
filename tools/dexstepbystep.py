from rpcclient import *
from sql import *

dbInit()


missing=["28d951ac4b48c31706561dac62fd0ea065a7873faede5316ff04817e1057315f",
"a4c8894bd129a801c07976989ce1a2f81b999434f1ab29b9edd2b13c245099c7",
"3f16eb239646236ae53e577d8221a837d23b322f7136776d246ea242e65c07b0",
"f3238944a65a755fcd6a75d1df70407e31b7c9c8ffba2c54daeb902eb468cfe2",
"7e0e0bebe8d64d49dfb41155028636be0ac16995208cd95eab28a6e72af0df91",
"a8da1665b10e6f35f42d40472feb8bec992ad4bd50cb08bfa0a30c5cd704fb34",
"18ceea2b279304404584079b8347b03721e1d09cee2c6bca8251f15f2559f535",
"048559218ff32a2afe153b4db4c0a24c8e315130472e94f737f47c69e80fe28f",
"7c26b67f12cbfcd583ceb861c3fcfa92d5bc037f71a2d2e8efbac823c4b45eb9",
"4ea05d85ab401b7382f69b7a0d34dc38d50ea58b20f0d0386ffa2cd5e26e5c69",
"fa91b1baadfa47d48c7ab73dfb7ed6927d503add024211fb521f660470f353f9",
"2dad05c732b3f555d3ff7da4167a50f22a39ab3d92712e013aa1140fb75afab4",
"eb9797769cb61b1f4313062c2a6d45a8787a171406e3f546ffab57725fcee6f7",
"788e2ff5fee14245466745279ee03b68b4f3fda6387721bf56f5c447f40c2a27",
"5acf11630be5373f708f70ff7bf9d842311c78190ce5dd70d208f47a7f23a860",
"f83d643563932bb181457b3d3dd64de56515d938dc345eaa72ea945869b6d848",
"5256991ce6bdf40f068de1a46d5800bb63e1a0f3a580b6344496b32848c495e2"]

Protocol="Mastercoin"

for tx in missing:

  x=dbSelect("select txblocknumber,txdbserialnum,txtype from transactions where txhash=%s order by txdbserialnum", [tx])

  Block=x[0][0]
  TxDBSerialNum=x[0][1]
  type=x[0][2]
  print "Processing", TxDBSerialNum

  rawtx={}
  rawtx['result']=json.loads(dbSelect("select txdata from txjson where txdbserialnum=%s", [TxDBSerialNum])[0][0])

  print "Block:", Block, ", TxDbSerialNum:", TxDBSerialNum, ", type:", type, ", rawtx:", json.dumps(rawtx, indent=2)

  expireAccepts(Block)

  if type == 20:
    if rawtx['result']['valid']:
      updatedex(rawtx, TxDBSerialNum)

  elif type == 22:
    offerAccept(rawtx, TxDBSerialNum, Block)

  elif type == -22:
    Sender=rawtx['result']['sendingaddress']
    for payment in rawtx['result']['purchases']:
      Receiver = payment['referenceaddress']
      PropertyIDBought = payment['propertyid']
      Valid=payment['valid']

      if Valid:
        if getdivisible_MP(PropertyIDBought):
         AmountBought=int(decimal.Decimal(payment['amountbought'])*decimal.Decimal(1e8))
        else:
          AmountBought=int(payment['amountbought'])
        updateAccept(Sender, Receiver, AmountBought, PropertyIDBought, TxDBSerialNum)

  dbCommit()
  raw_input('Processed, Press <ENTER> to continue')



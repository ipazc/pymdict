from pymdict.mongo_dict import MongoDict, BasicMongoDict

m = MongoDict("r2d2", mongo_host="172.17.0.1", mongo_port=27015)

m['hi'] = "hello"
m['ho'] = "hola"

#any = m.fork("any")
any = MongoDict("any", mongo_host="172.17.0.1", mongo_port=27015)
any['hi'] = "ha"
del any['hi']

for x in any("value % h"):
    print(x)

"""
s = MongoDict("any", mongo_host="172.17.0.1", mongo_port=27015)
s['hi'] = "ha"

#del s['hi']

for x in m("value % h"):
    print(x)

for x in s("value % h"):
    print(x)

"""
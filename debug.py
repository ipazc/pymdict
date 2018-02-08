from pymdict.mongo_dict import MongoDict, BasicMongoDict

m = MongoDict("r2d2", mongo_host="172.17.0.1", mongo_port=27015)

b = BasicMongoDict("___MONGO_DICT_META___", mongo_host="172.17.0.1", mongo_port=27015)


m['example'] = "hola"
m['example2'] = "lala"

fork = m.fork("r2d3")

print(fork['example'])
print(fork['example2'])

del fork['example']

#print(fork['example'])
print(fork['example2'])

print("fork keys", fork.keys())
print("m keys", m.keys())
print(m['example'])
print(m['example2'])

"""
print(b['r'])


print(type(m))

fork = m.fork("r2d2")

print(type(fork))
"""
print(type(m), m)

#m['example'] = "jaja"
#fork = m.fork("exampleid2")
#print(m['example'])
#m['example'] = "jojo"
#print(fork['example'])
"""m2 = ForkedMongoDict(m, original_dict_id="c", mongo_host="172.17.0.1", mongo_port=27015)
m['example2'] = {'hi': 61}
print(m['example2'])
print(m2['example2'])
m['example2'] = {'hi': 62}
print(m2['example2'])
b = BasicMongoDict("___MONGO_DICT_META___", mongo_host="172.17.0.1", mongo_port=27015)

print(b['c'])
"""
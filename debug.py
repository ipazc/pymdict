from pymdict.mongo_dict import MongoDict, DictDropper

m = MongoDict("ex", mongo_host="172.17.0.1", mongo_port=27015)

m["test"] = "test"
m["test2"] = "test2"

fork1 = m.fork("fork1")

fork1['test3'] = 'test3'

fork2 = fork1.fork("fork2")

print(fork2.keys())
print(fork1.keys())
print(m.keys())

print(fork1)

d = DictDropper(mongo_host="172.17.0.1", mongo_port=27015)
d.drop_dict('ex')
d.drop_dict('fork1')
d.drop_dict('fork2')





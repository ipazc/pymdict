from pymdict.mongo_dict import MongoDict, ForkedMongoDict

m = MongoDict("a", mongo_host="172.17.0.1", mongo_port=27015)

m2 = ForkedMongoDict(m, original_dict_id="c", mongo_host="172.17.0.1", mongo_port=27015)

print(m['example2'])
print(m2['example2'])
m['example2'] = {'hi': 62}
print(m2['example2'])

from poc.mongo_query_parser import MongoQueryParser

m = MongoQueryParser()
print(m._do_split("val.hola = 22 and (val.pepe > 44 or val.juan < 44)", open_splits=["(", "[", "'"], close_splits=[")", "]", "'"],
          special_separators="["))

print(m._do_split("(val.pepe > 44 or val.juan < 44) and val.hola = 22", open_splits=["(", "[", "'"], close_splits=[")", "]", "'"],
          special_separators="["))

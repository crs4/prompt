import pickle
import json
import os

class Serializer(object):

    def __init__(self):
        pass

    def serialize_as_string(self, obj):
        return pickle.dumps(obj)

    def deserialize_from_string(self, obj):
        return pickle.loads(obj)

    def serialize_as_file(self, obj, filename):
        try:
            with open(filename, 'w') as handle:
                pickle.dump(obj, handle)
            return True
        except Exception, e:
            return False

    def deserialize_from_file(self, filename):
        try:
            with open(filename, 'r') as handle:
                obj = pickle.load(handle)
            return obj
        except Exception, e:
            return False

    def serialize_as_json_file(self, obj, filename):
        try:
            file_path = os.getcwd()+'/'+filename
            json_file = open(file_path, 'w')
            json.dump(obj, json_file, sort_keys=True, indent=2)
            json_file.close()
            return True
        except Exception, e:
            print("An error occurred while serializing to "+file_path)
            print(str(e))
            return False

    def deserialize_json_from_file(self, filename):
        try:
            file_path = os.getcwd()+'/'+filename
            json_file = open(file_path, 'r')
            obj = json.load(json_file)
            json_file.close()
            return obj
        except Exception, e:
            print("An error occurred while deserializing "+file_path)
            print(str(e))
            return False
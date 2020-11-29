import hashlib

from setting.setting_system import COLLECTION_NAME, HOST, PORT
from src.mongo_client import ManagerMongoDb


class WorkerStorageTable:
    def __init__(self, mongo_client=ManagerMongoDb(host=HOST, port=PORT)):
        self.__mongo_client = mongo_client

    @staticmethod
    def __get_hash(value: str) -> str:
        data_hash = hashlib.sha256(str(value).encode('utf-8')).hexdigest()
        return data_hash

    @staticmethod
    def __get_data_file(data_hash: str,
                        files: list,
                        name_storage: str,
                        num_str_storage: int,
                        number_repetitions: int) -> dict:
        data_file = {
            "_id": data_hash,
            "files": files,
            "storage": {
                "name": name_storage,
                "num": num_str_storage
            },
            "number_repetitions": number_repetitions,
        }

        return data_file

    def insert_data_hash_in_mongo(self,
                                  data: str,
                                  name_file: str,
                                  num_str_file: int,
                                  name_storage: str,
                                  positions_segment: int,
                                  ) -> None:

        data_hash = self.__get_hash(data)
        manager_mongo = ManagerMongoDb(host=HOST, port=PORT)

        data_file = self.__get_data_file(data_hash=data_hash,
                                         files=[{
                                             "name": name_file,
                                             "num": num_str_file
                                         }],
                                         name_storage=name_storage,
                                         num_str_storage=positions_segment,
                                         number_repetitions=0)

        manager_mongo.insert_document(name_collection=COLLECTION_NAME,
                                      data=data_file)

    def update_data_hash_in_mongo(self,
                                  data: str,
                                  name_file: str,
                                  num_str_file: int,
                                  num_str_storage: int,
                                  ) -> None:

        data_hash = self.__get_hash(data)
        manager_mongo = ManagerMongoDb(host=HOST, port=PORT)

        doc = manager_mongo.find_document(
            collection_name=COLLECTION_NAME,
            elements={"_id": data_hash},
            multiple=False)

        # Issue PyCharm: https://youtrack.jetbrains.com/issue/PY-36633
        doc['files'].append({
            "name": name_file,
            "num": num_str_file
        })

        # Issue PyCharm: https://youtrack.jetbrains.com/issue/PY-36633
        data_file = self.__get_data_file(data_hash=data_hash,
                                         files=doc["files"],
                                         name_storage=doc["storage"]["name"],
                                         num_str_storage=num_str_storage,
                                         number_repetitions=int(doc["number_repetitions"]) + 1)

        manager_mongo.update_document(collection_name=COLLECTION_NAME,
                                      query_elements={"_id": data_hash},
                                      new_values=data_file)

    def check_hash(self, data: str) -> bool:
        manager_mongo = ManagerMongoDb(host=HOST, port=PORT)
        data_hash = self.__get_hash(data)
        doc = manager_mongo.find_document(
            collection_name=COLLECTION_NAME,
            elements={"_id": data_hash},
            multiple=False)

        if doc is None:
            return False
        else:
            return True

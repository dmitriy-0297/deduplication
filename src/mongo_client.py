from pymongo import MongoClient
from typing import Union, Any

from setting.setting_system import (
    DB_NAME,
)


class ManagerMongoDb:
    def __init__(self, host: str, port: str):
        self.__client = MongoClient(host=host + ':' + port)
        self.__db = None

    def __connect_db(self) -> None:
        self.__db = self.__client[DB_NAME]

    def get_collection(self, name_collection: str) -> Any:
        self.__connect_db()
        collection = self.__db[name_collection]
        return collection

    def insert_document(self, name_collection: str, data: dict) -> str:
        collection = self.get_collection(name_collection)
        return collection.insert_one(data).inserted_id

    def find_document(self,
                      collection_name: str,
                      elements: dict,
                      multiple=False) -> Union[list, Any]:

        collection = self.get_collection(collection_name)

        if multiple:
            results = collection.find(elements)
            return [r for r in results]
        else:
            return collection.find_one(elements)

    def update_document(self,
                        collection_name: str,
                        query_elements: dict,
                        new_values: dict) -> None:

        collection = self.get_collection(collection_name)
        collection.update_one(query_elements, {'$set': new_values})

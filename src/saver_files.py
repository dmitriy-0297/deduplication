from binascii import unhexlify
from typing import List

from src.mongo_client import ManagerMongoDb
from setting.setting_system import (
    HOST,
    PORT,
    COLLECTION_NAME,
    DIRECTORY_FILES_STORAGE,
    DIRECTORY_FILE_RECOVERY
)


class SaverFiles:
    def __init__(self, file_name):
        self.__file_name = file_name

    @staticmethod
    def __get_data_file_storage(storage_name: str) -> List:
        file_storage = open(DIRECTORY_FILES_STORAGE + storage_name, 'rb')
        return file_storage.readlines()

    def __write_file(self, pack: list):
        with open(DIRECTORY_FILE_RECOVERY + self.__file_name, 'wb') as file:
            for i in pack:
                file.write(unhexlify(i.strip()))

        print('-- WRITE FILE')

    def __get_record_by_file_name(self) -> List:
        manager_mongo = ManagerMongoDb(host=HOST, port=PORT)
        elements = manager_mongo.find_document(collection_name=COLLECTION_NAME,
                                               elements={
                                                   'files.name': self.__file_name
                                               },
                                               multiple=True)

        print('-- GET SORTED DATA')
        return sorted(elements, key=lambda k: int(k['files'][0]['num']))

    def recover_file(self) -> None:
        data_list = self.__get_record_by_file_name()
        pack = []

        for data in data_list:
            storage_name = data["storage"]["name"]
            storage_num = int(data["storage"]["num"])

            list_strings = self.__get_data_file_storage(storage_name=storage_name)

            # list index [0...x], store_num [1...x]
            element = list_strings[storage_num - 1]
            pack.append(element)

        print('-- GET LIST BYTES')

        self.__write_file(pack=pack)

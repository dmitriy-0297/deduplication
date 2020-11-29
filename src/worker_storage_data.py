from typing import List

from setting.setting_system import DIRECTORY_FILES_STORAGE
from src.worker_storage_table import WorkerStorageTable


class WorkerStorageData:
    @staticmethod
    def __write_file(data: List, file_name: str) -> None:
        with open(file_name, "wb+") as file:
            for i in data:
                file.write(i + b'\n')

    @staticmethod
    def __read_file(path_file: str) -> str:
        with open(path_file, "r") as file:
            for line in file:
                return line

    def write_storage(self, arr_data: List, file_name: str) -> None:
        worker_storage_table = WorkerStorageTable()
        write_list_data = []
        count_line_storage_update = 1
        count_line_storage_insert = 1

        for data in arr_data:
            if worker_storage_table.check_hash(data=data['bytes']) is True:
                worker_storage_table.update_data_hash_in_mongo(data=data['bytes'],
                                                               name_file=file_name,
                                                               num_str_file=data['positions_segment'],
                                                               num_str_storage=count_line_storage_update)
                count_line_storage_update += 1
            else:
                worker_storage_table.insert_data_hash_in_mongo(data=data['bytes'],
                                                               name_file=file_name,
                                                               num_str_file=data['positions_segment'],
                                                               name_storage=file_name,
                                                               positions_segment=count_line_storage_insert)

                count_line_storage_insert += 1
                write_list_data.append(data['bytes'])

        if write_list_data:
            self.__write_file(data=write_list_data, file_name=DIRECTORY_FILES_STORAGE + file_name)

        print("-- DATA SAVED IN STORAGE")

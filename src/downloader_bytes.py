import binascii

from setting.setting_system import (
    DIRECTORY_FILE,
)
from src.worker_storage_data import WorkerStorageData


class DownloaderBytes:
    def __init__(self, file_name):
        self.__file_name = file_name

    def __read_file(self, num_bytes: int) -> list:
        positions_segment = 1
        arr_bytes = []

        print('-- READ FILE: ', self.__file_name)
        with open(DIRECTORY_FILE + self.__file_name, "rb") as file:
            while True:
                chunk = file.read(num_bytes)
                if chunk:
                    arr_bytes.append({
                        "bytes": binascii.hexlify(chunk),
                        "positions_segment": positions_segment
                    })
                    positions_segment += 1
                else:
                    break

        return arr_bytes

    def __write_storage(self, arr_bytes):
        print('-- WRITE STORAGE')
        worker_storage_data = WorkerStorageData()
        worker_storage_data.write_storage(arr_data=arr_bytes,
                                          file_name=self.__file_name)

    def save_data_bytes(self, num_bytes: int):
        arr_bytes = self.__read_file(num_bytes=num_bytes)
        self.__write_storage(arr_bytes=arr_bytes)

        print("-- SAVE DATA IN STORAGE: ", self.__file_name)

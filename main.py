import argparse
import time

from src.downloader_bytes import DownloaderBytes
from src.saver_files import SaverFiles

parser = argparse.ArgumentParser()

parser.add_argument('--mode',
                    type=int,
                    help='1 - recovery_files bytes in storage or 2 - restoring a file',
                    default=1,
                    action='store')

parser.add_argument('--file',
                    type=str,
                    help='name file from directory /files/',
                    default='test_1.txt',
                    action='store')

parser.add_argument('--butes',
                    type=int,
                    help='number of bytes for read from stream',
                    default=4,
                    action='store')

if __name__ == "__main__":
    args = parser.parse_args()
    mode = args.mode
    file = args.file
    num_bytes = args.butes

    if mode == 1:
        print('-- MODE_1: RECOVERY FILE BYTES:')

        start_time = time.time()
        downloader = DownloaderBytes(file_name=file)
        downloader.save_data_bytes(num_bytes=num_bytes)
        print("--- %s seconds ---" % (time.time() - start_time))
    elif mode == 2:
        print('-- MODE_2: RESTORING A FILE:')

        start_time = time.time()
        recover_files = SaverFiles(file_name=file)
        recover_files.recover_file()
        print("--- %s seconds ---" % (time.time() - start_time))
    else:
        print("No mode entered!!")
        exit(1)

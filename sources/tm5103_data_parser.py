import os
import time
from datetime import datetime, timedelta



class TM5103DataParser:

    def __create_output_dir(self, dir_name):
        if dir_name not in os.listdir():
            try:
                os.mkdir(dir_name)
                print(f"<{dir_name}> has been created")
            except OSError:
                print(f"Can't create <{dir_name}> directory")
        else:
            print(f"<{dir_name}> already exists")

    def __make_title(self, date):
        return '%s.txt' % '_'.join(reversed(date.split('.')))


    # !Переделать функцию, так как она не очевидна - записывает в файл, но возвращает ссылку на файл
    def __write_first_line(self, data, dest_dir):
        try:
            output_file = self.__make_title(data[0])
            str_data = '\t'.join(data[1:])
            w = open(f'{dest_dir}/{output_file}', 'w')
            w.write(f'{str_data}\n')
        except IOError:
            print(f'I/O error with <{output_file}>!')
        return w

    def parse_file(self, filename, output_dir):
        print(f'Starting split of <{filename}> for data files.\n...')
        start_time = time.perf_counter()
        self.__create_output_dir(output_dir)
        try:
            with open(filename, 'r') as f:
                data = f.readline().split()
                cur_date = data[0]
                w = self.__write_first_line(data, output_dir)
                for line in f:
                    data = line.split()
                    if data[0] == cur_date:
                        str_data = '\t'.join(data[1:])
                        try:
                            w.write(f'{str_data}\n')
                        except IOError:
                            output_file = self.__make_title(data[0])
                            print(f'I/O error with <{output_file}>!')
                    else:
                        try:
                            w.close()
                        except IOError:
                            output_file = self.__make_title(data[0])
                            print(f'I/O error with <{output_file}>!')
                        cur_date = data[0]
                        w = self.__write_first_line(data, output_dir)
            parse_time = time.perf_counter() - start_time
            ms_time = round(parse_time * 1e3, 3)
            print(f'<{filename}> has been processed in {ms_time} ms.')
        except IOError:
            print(f'I/O error. Please, check <{filename}>.')

    def write_data_to_file(self, data, filename):
        try:
            with open(filename, 'w') as w:
                w.write('\n'.join(['\t'.join(line) for line in data]))
        except IOError:
            print(f'I/O error with <{filename}>.')

    def split_file(self, filename, channel_count):
        result = dict()
        print(f'Starting split of <{filename}> for data files.\n...')
        start_time = time.perf_counter()
        try:
            with open(filename, 'r') as f:
                cur_date = None
                for line in f:
                    data = line.split()
                    if data[0] != cur_date:
                        cur_date = data[0]
                        result[cur_date] = [data[1:channel_count+2]]
                    else:
                        result[cur_date].append(data[1:channel_count+2])
        except IOError:
            print(f'I/O error with <{filename}>.')
        for key, value in result.items():
            title = f'data_files/{self.__make_title(key)}'
            self.write_data_to_file(value, title)
        parse_time = time.perf_counter() - start_time
        ms_time = round(parse_time * 1e3, 3)
        print(f'<{filename}> has been processed in {ms_time} ms.')
        # print(result)
        return result

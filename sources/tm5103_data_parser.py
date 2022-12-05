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

    def __parse_line(self, line):
        return [el for el in line.rstrip().split(' ') if el]


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
                data = self.__parse_line(f.readline())
                cur_date = data[0]
                w = self.__write_first_line(data, output_dir)
                for line in f:
                    data = self.__parse_line(line)
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

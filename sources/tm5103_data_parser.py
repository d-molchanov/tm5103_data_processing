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
        # return '%s.txt' % '_'.join(reversed(date.split('.')))
        return '_'.join(reversed(date.split('.')))


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
                w.write('\n'.join([';'.join(line) for line in data]))
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

    def extract_single_date(self, filename, date):
        data = []
        try:
            with open(filename, 'r') as f:
                data = [line.split()[1:] for line in f if date in line]
        except IOError:
            print(f'I/O error with <{filename}>.')
        return data

    def extract_columns(self, filename, columns):
        data = []
        try:
            with open(filename, 'r') as f:
                # for line in f:
                #     line_list = line.split()
                #     data.append([line_list[c] for c in columns])
                data = [[line.split()[c] for c in columns] for line in f]
        except IOError:
            print(f'I/O error with <{filename}>.')
        return data

    def convert_to_float(self, _str):
        try:
            return float(_str.replace(',', '.'))
        except ValueError:
            return None

    def parse_time(self, _time):
        return datetime.strptime(_time, '%H:%M:%S')

    def extract_data(self, str_data):
        data = [
            [self.parse_time(line[0])] + 
            [self.convert_to_float(el) for el in line[1:]] for 
            line in str_data
        ]
        return data

    def reduce_data(self, data, number_of_lines):
        return [line for i, line in enumerate(data) if i % number_of_lines == 0]

    def extract_columns_new(self, data, columns):
        return [[line[c] for c in columns] for line in data]

    def create_new_filename(self, filename, suffix):
        path, fname = os.path.split(filename)
        name, ext = os.path.splitext(fname)
        return os.path.join(path, f'{name}_{suffix}.csv')

    def define_reactor(self, filename, substitution):
        name, ext = os.path.splitext(filename)
        suffix = substitution.get(name)
        if suffix:
            return f'{suffix}.csv'
        else:
            return filename

    def process_experiment(self, filename, date, substitution):
        raw_data = self.extract_single_date(filename, date)
        prefix = self.__make_title(date)
        path, fname = os.path.split(filename)
        suffix = self.define_reactor(fname, substitution)
        date_filename = os.path.join(path, f'{prefix}_{suffix}')
        self.write_data_to_file(raw_data, date_filename)
        columns = list(range(9))
        col_data = self.extract_columns_new(raw_data, columns)
        self.write_data_to_file(col_data, self.create_new_filename(date_filename, 'c'))
        reduced_data = self.reduce_data(col_data, 27)
        self.write_data_to_file(reduced_data, self.create_new_filename(date_filename, 'reduced'))

        # print(*reduced_data, sep='\n')


data_parser = TM5103DataParser()
# filename = './../(2023_09_22)_RA.txt'
# filename = 'D:/JIHT/!2023/!Ларина/!Raw_ED/(2023_09_28)_Pyrocarbon/Reactor A/ARHrep/TM5103-4217863/StandartConfig/DB/230928141800/All_Chan.txt'
filenames = [
    'D:/JIHT/!2023/!Ларина/!Processed_ED/tm5103-4217863.txt',
    'D:/JIHT/!2023/!Ларина/!Processed_ED/tm5103-4217905.txt',
]
substitution = {'tm5103-4217863': 'A', 'tm5103-4217905': 'B'}
# date = '22.09.2023'
date = '16.11.2023'
for f in filenames:
    data_parser.process_experiment(f, date, substitution)

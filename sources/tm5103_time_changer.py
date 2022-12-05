import os
import time
from datetime import datetime, timedelta


class TM5103TimeChanger:


    def set_separator(self, separator):
        self.__sep = separator


    def set_time_format(self, time_format):
        self.__time_format = time_format


    def __parse_line(self, line):
        first_tab_index = line.find(self.__sep)
        if first_tab_index >= 0:
            return (
                line[:first_tab_index],
                line[first_tab_index+1:]
                )
        else:
            return None

    def __parse_time(self, timestamp):
        try:
            return datetime.strptime(
                f'{timestamp}', self.__time_format
            )
        except ValueError:
            print('{0} {1}'.format(
                f'Mismatch time format: {timestamp}',
                f'does not suit <{self.__time_format}>'))
            return None

    def __replace_time(self, line, timestamp):
        data = self.__parse_line(line)
        if data:
            old_timestamp = self.__parse_time(data[0])
            if old_timestamp:
                return '{0}{1}{2}'.format(
                    datetime.strftime(timestamp, self.__time_format),
                    separator,
                    data[1])
            else:
                return '{0} {1}'.format(
                    f'Mismatch time format: {timestamp}',
                    'does not suit <23:59:59>')
        else:
            return '{0} {1}'.format(
                f'Mismatch separator <{self.__sep}>',
                ' - cannot parse line')    

    def __rename_file(self, filename):
        try:
            dot_index = filename.rfind('.', -5)
            return '{0}_t{1}'.format(
                filename[:dot_index],
                filename[dot_index:])
        except ValueError:
            return f'{filename}_t'

    def __create_line(self, timestamp, data):
        try:
            return '{0}{1}{2}'.format(
                datetime.strftime(timestamp, self.__time_format),
                self.__sep,
                data)
        except ValueError:
            print('Cannot convert datetime to string')
            return f'Error:{timestamp}: {data}'


    def __write_file(self, filename, 
        new_timestamp, firts_timestamp, f, data):
        try:
            with open(filename, 'w') as w:
                nts = self.__parse_time(new_timestamp)
                td = firts_timestamp - nts
                w.write(self.__create_line(nts, data[1]))
                for line in f:
                    data = self.__parse_line(line)
                    if data:
                        timestamp = self.__parse_time(data[0])
                        new_time = timestamp - td
                        w.write(self.__create_line(new_time, data[1]))
        except IOError:
            print(f'Writing error: <{filename}>')


    def change_time(self, filename, new_timestamp):
        print(f'Starting time change of <{filename}>.\n...')
        start_time = time.perf_counter()
        try:
            with open(filename, 'r') as f:
                data = self.__parse_line(f.readline())
                if len(data) == 2:
                    first_timestamp_str = data[0]
                    firts_timestamp = self.__parse_time(
                        first_timestamp_str
                    )
                    output_file = self.__rename_file(filename)
                    if firts_timestamp:
                        self.__write_file(
                            output_file, new_timestamp,
                            firts_timestamp, f, data)    
                else:
                    print(' '.join((
                        'Format error at first line in', 
                        f'<{filename}>! Please, check it.')))
                parse_time = time.perf_counter() - start_time
                ms_time = round(parse_time * 1e3, 3)
                print(f'<{filename}> has been processed in {ms_time} ms.')        
        except IOError:
            print(f'I/O error. Please, check <{filename}>.')

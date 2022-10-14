import os
import time
from datetime import datetime, timedelta


class TM5103TimeChanger:


    def __parse_line(self, line, separator):
        first_tab_index = line.find(separator)
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
                f'{timestamp}', '%H:%M:%S'
            )
        except ValueError:
            print(f'Mismatch time format: {timestamp} does not suit <23:59:59>')
            return None

    def change_time(self, filename, new_timestamp):
        print(f'Starting time change of <{filename}>.\n...')
        start_time = time.perf_counter()
        try:
            with open(filename, 'r') as f:
                data = self.__parse_line(f.readline(), '\t')
                if len(data) == 2:
                    first_timestamp_str = data[0]
                    firts_timestamp = self.__parse_time(
                        first_timestamp_str
                    )
                    if firts_timestamp:
                        with open(f'{filename[:-4]}_tc.{filename[-3:]}', 'w') as w:
                            nts = self.__parse_time(new_timestamp)
                            td = firts_timestamp - nts
                            w.write(f'{datetime.strftime(nts, "%H:%M:%S")}\t{data[1]}')
                            for line in f:
                                data = self.__parse_line(line, '\t')
                                if data:
                                    timestamp = self.__parse_time(data[0])
                                    new_time = timestamp - td
                                    w.write(f'{datetime.strftime(new_time, "%H:%M:%S")}\t{data[1]}')
                else:
                    print(' '.join((
                        'Format error at first line in', 
                        f'<{filename}>! Please, check it.')))        
        except IOError:
            print(f'I/O error. Please, check <{filename}>.')

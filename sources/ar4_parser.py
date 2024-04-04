from typing import List, Tuple, Dict, Optional, Union

from datetime import datetime, timedelta
from time import perf_counter
import struct
import csv

class Ar4Parser():

    def __init__(self):
        self.chunk_size = 256
        self.empty_byte = b'\xff'
        self.file_sep = ';'
        self.datetime_format = '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}' 
        self.file_ext = 'csv'

    def config_parser(self, config: Dict[str, Union[str, int]]) -> None:
        if 'chunk_size' in config:
            self.chunk_size = config['chunk_size']
        if 'empty_byte' in config:
            self.empty_byte = config['empty_byte']
        if 'file_sep' in config:
            self.file_sep = config['file_sep']
        if 'datetime_format' in config:
            self.datetime_format = config['datetime_format']
        if 'file_ext' in config:
            self.file_ext = config['file_ext']
        return None

    def read_in_chunks(self, filename: str, chunk_size: int) -> List[bytes]:
        time_start = perf_counter()
        result = []
        try:
            with open(filename, 'rb') as f:
                chunk = f.read(chunk_size)
                while chunk:
                    result.append(chunk)
                    chunk = f.read(chunk_size)
                print('Read <{}> in {:.2f} ms.'.format(
                    filename, (perf_counter() - time_start)*1e3))
        except IOError as err:
            print(f'Error with <{filename}>:\n{err}.')
        return result

        # Check working with full archive!
    def cut_off_empty_tail(self, chunks:List[bytes], chunk_size:int, empty_byte:bytes) -> List[bytes]:
        result: List[bytes] = []
        if not chunks:
            return result

        chunk_with_no_data = chunk_size*empty_byte
        for i, ch in enumerate(chunks[::-1]):
            if ch != chunk_with_no_data:
                result = chunks[:len(chunks) - i]
                break
        return result

    def extract_data_chunks(self, binary_data: bytes, empty_byte: bytes) -> List[bytes]:
        result = []
        i = 0
        while i < len(binary_data):
            # if binary_data[i] != empty_byte[0]:
            if binary_data[i] != 255:
                msg_length = binary_data[i+1]
                result.append(binary_data[i:i+msg_length])
                i += msg_length
            else:
                i += 1
                # break
        return result


    def read_binary_file(self, filename: str, chunk_size: int,  empty_byte: bytes) -> Dict[str, Union[List[bytes], bytes, None]]:
    
        big_chunks = self.read_in_chunks(filename, chunk_size)
        if not big_chunks:
            return {'header': None, 'data': []}

        data_chunks = self.cut_off_empty_tail(big_chunks, chunk_size, empty_byte)
        reduced_chunks = []
        for chunk in data_chunks[1:]:
            reduced_chunks.extend(self.extract_data_chunks(chunk, empty_byte))
        
        return {'header': big_chunks[0], 'data': reduced_chunks}

    def split_prefix_and_reading(self, binary_data: List[bytes], empty_byte: bytes) -> Dict[str, List[bytes]]:
        if not binary_data:
            return {}
        not_datetime = 4*empty_byte

        split_index = 0
        for i, line in enumerate(binary_data):
            if line[2:6] != not_datetime:
                split_index = i
                break
        
        return {'prefix': binary_data[:split_index], 
                'readings': binary_data[split_index:]}

    def parse_ar4_file(self, filename: str, _chunk_size:Optional[int]=None, _empty_byte:Optional[bytes]=None) -> Union[Dict[str, List[bytes]], Dict[str, int]]:
        empty_byte = (_empty_byte or self.empty_byte)
        chunk_size = (_chunk_size or self.chunk_size)
        binary_data = self.read_binary_file(filename, chunk_size, empty_byte)
        processed_data = self.split_prefix_and_reading(binary_data['data'], empty_byte)
        # binary_data = bd['data']
        prefix = processed_data['prefix']
        readings = processed_data['readings']
        metadata = self.get_metadata(binary_data['header'], readings)
        return {'metadata': metadata, 'prefix': prefix, 'readings': readings}

    def get_tm_datetime(self, binary_data: bytes) -> Tuple[int, int, int, int, int, int]:
        dt_int = struct.unpack('<I', binary_data)[0]
        mask = [0b111111, 0b111111, 0b11111, 0b11111, 0b1111, 0b11111]
        shift = [0, 6, 12, 17, 22, 26]
        dt = [dt_int>>s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        # return datetime(*dt[::-1])
        return tuple(dt[::-1])

    def get_metadata(self, header: bytes, readings: List[bytes]) -> Dict[str, int]:
        creation_datetime = self.get_tm_datetime(header[22:26])
        unit_number = struct.unpack('<I', header[26:30])[0]
        min_and_max_timestamps = self.find_min_and_max_timestamps(readings)
        metadata = {'creation_datetime': creation_datetime, 'unit_number': unit_number}
        metadata.update(min_and_max_timestamps)
        print(metadata)
        return metadata

    def show_metadata(self, metadata: Union[Dict[str, int], Dict[str, str], Dict[str, Tuple[int, int, int, int, int, int]]]) -> None:
        print(f"\nUnit number: {metadata['unit_number']}\n")
        str_list = ['{:02d}.{:02d}.{:d}'.format(*metadata['creation_datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*metadata['creation_datetime'][3:])]
        print('Creation timestamp:\t{}'.format(' '.join(str_list)))
        self.show_min_and_max_timestamps(metadata)

    def find_min_and_max_timestamps(self, binary_data: List[bytes]) -> Dict[str, Tuple[int, int, int, int, int, int]]:
        max_timestamp = b'\x00\x00\x00\x01'
        min_timestamp = b'\xff\xff\xff\xff'
        for chunk in binary_data:
            timestamp = chunk[2:6][::-1]
            if timestamp > max_timestamp:
                max_timestamp = timestamp
            if timestamp < min_timestamp:
                min_timestamp = timestamp
        return {
            'min_timestamp': self.get_tm_datetime(min_timestamp[::-1]), 
            'max_timestamp': self.get_tm_datetime(max_timestamp[::-1])}


    def show_min_and_max_timestamps(self, ts: Dict[str, Tuple[int, int, int, int, int, int]]) -> None:
        str_list = ['{:02d}.{:02d}.{:d}'.format(*ts['min_timestamp'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*ts['min_timestamp'][3:])]
        # str_list = ['{:02d}.{:02d}.{:d}'.format(*self.get_tm_datetime(ts['min_timestamp'])[2::-1]),
        # '{:02d}:{:02d}:{:02d}'.format(*self.get_tm_datetime(ts['min_timestamp'])[3:])]
        print('Minimum timestamp:\t{}'.format(' '.join(str_list)))
        str_list = ['{:02d}.{:02d}.{:d}'.format(*ts['max_timestamp'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*ts['max_timestamp'][3:])]
        # str_list = ['{:02d}.{:02d}.{:d}'.format(*self.get_tm_datetime(ts['max_timestamp'])[2::-1]),
        # '{:02d}:{:02d}:{:02d}'.format(*self.get_tm_datetime(ts['max_timestamp'])[3:])]
        print('Maximum timestamp:\t{}'.format(' '.join(str_list)))

    def extract_one_date(self, binary_data: List[bytes], timestamp: Tuple[int, int, int, int, int, int]) -> List[bytes]:
        start_timestamp = timestamp[:3] + (0, 0, 0)
        try:
            end_timestamp = tuple((datetime(*start_timestamp)+timedelta(days=1)).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong timestamp: {err}')
        return self.extract_time_period(binary_data, start_timestamp, end_timestamp)            


    def get_bits(self, binary_data: int, n: int) -> List[int]:
        result = []
        for i in range(n):
            result.append(binary_data >> i & 1)
        return result[::-1]

    def convert_to_float(self, binary_data: bytes) -> float:
        return struct.unpack('!f',binary_data)[0]

    #!Метод жестко привязан к длине chunk в 42 байта. Вообще надо вынести декодирование значений в отдельный метод.
    def decrypt_data(self, binary_data: bytes) -> Dict[str, list]:
        indices = [0, 2, 6, 7, 8, 9, 13, 17, 21, 25, 29, 33, 37, 41, 42]
        temp_data = [binary_data[i1:i2] for i1, i2 in zip(indices[:-1], indices[1:])]
        dt = self.get_tm_datetime(temp_data[1])
        limits = self.get_bits(ord(temp_data[2]), 8)[::-1]

        err = self.get_bits(ord(temp_data[4]), 8)[::-1]
        values = [self.convert_to_float(el) if not e else None for el, e in zip(temp_data[5:-1], err)]
        cs = binary_data[-1]
        return {'datetime': dt, 'values': values, 'errors': err, 'limits': limits, 'cs': cs}

    def process_chunks(self, binary_data: List[bytes]) -> List[dict]:
        if not binary_data:
            return []
        time_start = perf_counter()
        result = [self.decrypt_data(chunk) for chunk in binary_data]
        print('Data decrypted in {:.2f} ms.'.format(
            (perf_counter() - time_start)*1e3))
        time_start = perf_counter()
        r = sorted(result, key=lambda d: d['datetime'])
        print('Data sorted in {:.2f} ms.'.format(
            (perf_counter() - time_start)*1e3))
        return r

    def values_to_str(self, reading: dict, sep: str) -> str:
        str_data = ['{:02d}.{:02d}.{:d}'.format(*reading['datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*reading['datetime'][3:]),
        *['{:.6f}'.format(v).replace('.', ',') if v != None else 'None' for v in reading['values']]]
        return '{}\n'.format(sep.join(str_data))

    def write_file(self, data: List[dict], filename: str) -> None:
        try:
            with open(filename, 'w') as f:
                for line in data:
                    f.write(line)
        except IOError:
            print(f'Error with <{filename}>.')

    def extract_last_date(self, data: dict) -> List[bytes]:
        return self.extract_one_date(
            data['readings'], data['metadata']['max_timestamp'])

    def convert_timestamp_to_int(self, timestamp: Tuple[int, int, int, int, int, int]) -> int:
        return (
            (timestamp[5]) +
            (timestamp[4]<<6) +
            (timestamp[3]<<12) +
            ((timestamp[2]-1)<<17) + 
            ((timestamp[1]-1)<<22) + 
            ((timestamp[0] - 2000)<<26)
        )

    # Посмотреть перевод "начало временного интервала"
    def extract_time_period(self, binary_data: List[bytes], start_timestamp: Tuple[int, int, int, int, int, int], end_timestamp: Tuple[int, int, int, int, int, int]) -> List[bytes]:
        try:
            sts = tuple(datetime(*start_timestamp).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong start timestamp: {err}.')
        try: 
            ets = tuple(datetime(*end_timestamp).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong end timestamp: {err}.')
        start_ts = struct.pack('>I', self.convert_timestamp_to_int(sts))
        end_ts = struct.pack('>I', self.convert_timestamp_to_int(ets))

        d = []
        for chunk in binary_data:
            # ts = struct.unpack('<I', chunk[2:6])[0]
            # if start_ts <= chunk[2:6][::-1] < end_ts:
            if start_ts <= chunk[5:1:-1] < end_ts:
                d.append(chunk)
        # return self.process_chunks(d)
        return d

    def convert_bytes_to_int(self, binary_data: bytes) -> int:
        return (struct.unpack('<H', binary_data[4:6])[0] >> 1)
        # return (struct.unpack('<I', binary_data[2:6])[0] >> 17)


    def convert_int_to_date(self, int_date: int) -> Tuple[int, int, int, int, int, int]:
        mask = [0b11111, 0b1111, 0b11111]
        shift = [0, 5, 9]
        dt = [int_date>>s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        return tuple(dt[::-1])


    def split_dates(self, binary_data: List[bytes], write_files: bool=False) -> Dict[str, list]:
        if not binary_data:
            return {}
        result = {}
        for chunk in binary_data:
            ts = self.convert_bytes_to_int(chunk)
            # ts = self.get_tm_datetime(chunk[2:6])[:3]
            if ts in result:
                result[ts].append(chunk)
            else:
                result[ts] = [chunk]
        if write_files:
            for key in result:
                filename = '{:02d}_{:02d}_{:02d}.csv'.format(*self.convert_int_to_date(key))
                data = self.process_chunks(result[key])
                str_data = [self.values_to_str(chunk, ';') for chunk in data]
                self.write_file(str_data, filename)
        return result

    def create_filename(self, unit_number: int, start_timestamp: Tuple[int, int, int, int, int, int], end_timestamp: Tuple[int, int, int, int, int, int]) -> str:
        dt_format = '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}' 
        sts = dt_format.format(*start_timestamp)
        ets = dt_format.format(*end_timestamp)
        return '{}_{}-{}.csv'.format(unit_number, sts, ets)


    def if_write_file(self, processed_data: List[dict], unit_number: int, write_to_file: bool) -> None:
        output_filename = self.create_filename(unit_number, processed_data[0]['datetime'], processed_data[-1]['datetime'])
        if write_to_file:
            str_data = [self.values_to_str(el, ';') for el in processed_data]
            self.write_file(str_data, output_filename)

    def extract_last_date_from_outside(self, raw_data: dict, write_to_file: bool =False) -> List[dict]:
        data = self.extract_last_date(raw_data)
        processed_data = self.process_chunks(data)
        self.if_write_file(processed_data, raw_data['metadata']['unit_number'], write_to_file)
        return processed_data

    def extract_time_period_from_outside(self, raw_data: dict, start_timestamp: Tuple[int, int, int, int, int, int], end_timestamp: Tuple[int, int, int, int, int, int], write_to_file:bool =False):
        data = self.extract_time_period(raw_data['readings'], start_timestamp, end_timestamp)
        processed_data = self.process_chunks(data)
        self.if_write_file(processed_data, raw_data['metadata']['unit_number'], write_to_file)
        return processed_data


if __name__ == '__main__':
    
    filename = 'TM100514_B.AR4'
    write_to_file = True
    config = {
        'chunk_size': 256,
        'empty_byte': b'\xff',
        'file_sep': ';',
        'datetime_format': '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}',
        'file_ext': 'csv'
    }
    # output_filename = '2024_03_13_B.csv'
    start_timestamp = (2023, 10, 5)
    # start_timestamp = (2023, 10, 5)
    end_timestamp = (2023, 10, 6)
    # end_timestamp = (2023, 10, 5, 14, 49, 44)

    ar4_parser = Ar4Parser()
    ar4_parser.config_parser(config)
    raw_data = ar4_parser.parse_ar4_file(filename)
    # time_start = perf_counter()
    # processed_data = ar4_parser.extract_last_date_from_outside(raw_data, write_to_file=True)
    # processed_data = ar4_parser.extract_time_period_from_outside(raw_data, start_timestamp, end_timestamp, write_to_file=True)
    # print('Last date exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(processed_data)))
    
    chunks = ar4_parser.read_in_chunks(filename, 256)
    print(len(chunks))
    print(len(chunks[0]), len(chunks[-1]))
    print(chunks[0][:110])
    print(chunks[0].hex(), chunks[0], sep='\n')
    print('\n', chunks[1].hex())
    print('\n', chunks[2].hex())
    print(chunks[0][43:60])
    print()
    print(228642, chunks[228642].hex())
    print(228643, chunks[228643].hex())
    print(228644, chunks[228644].hex())
    # for i, ch in enumerate(chunks[::-1]):
    #     if ch != b'\xff'*256:
    #         print(ch)
    #         print(len(chunks)-i)
    #         break

    # ========================================old==============================
    # time_start = perf_counter()
    # data3 = ar4_parser.extract_time_period(raw_data['readings'], start_timestamp, end_timestamp)
    # print('Time period exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(data3)))
    # print(data1 == data3)
    # time_start = perf_counter()
    # data4 = ar4_parser.split_dates(raw_data['readings'])
    # data4 = ar4_parser.split_dates(data1, write_files=True)
    # data4 = ar4_parser.split_dates(raw_data['readings'], write_files=True)
    # print('Dates were splitted in {:.2f} ms. {} dates.'.format((perf_counter() - time_start)*1e3, len(data4)))

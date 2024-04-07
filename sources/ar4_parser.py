from typing import List, Tuple, Dict, Optional, Union

from datetime import datetime, timedelta
from time import perf_counter
import struct
import csv

class Ar4Parser():

    def __init__(self):
        self.chunk_size = 256
        self.empty_byte = b'\xff'
        self.channels_amount = 8
        self.file_sep = ';'
        self.datetime_format = '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}' 
        self.file_ext = 'csv'

    def config_parser(self, config: Dict[str, Union[str, int]]) -> None:
        if 'chunk_size' in config:
            self.chunk_size = config['chunk_size']
        if 'empty_byte' in config:
            self.empty_byte = config['empty_byte']
        if 'channels_amount' in config:
            self.channels_amount = config['channels_amount']
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

    def extract_records(self, binary_data: bytes, empty_byte: bytes) -> List[bytes]:
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
            return {'header': None, 'adc_records': []}

        data_chunks = self.cut_off_empty_tail(big_chunks, chunk_size, empty_byte)
        records = []
        for chunk in data_chunks[1:]:
            records.extend(self.extract_records(chunk, empty_byte))
        
        return {'header': big_chunks[0], 'adc_records': records}

    # Maybe change prefix to overhead 
    def split_prefix_and_records(self, adc_records: List[bytes], empty_byte: bytes) -> Dict[str, List[bytes]]:
        if not adc_records:
            return {}
        not_datetime = 4*empty_byte

        split_index = 0
        for i, line in enumerate(adc_records):
            if line[2:6] != not_datetime:
                split_index = i
                break
        
        return (adc_records[:split_index], adc_records[split_index:])

    def parse_ar4_file(self, filename: str, chunk_size:Optional[int]=None, 
        empty_byte:Optional[bytes]=None) -> Union[Dict[str, List[bytes]], Dict[str, int]]:
        _empty_byte = (empty_byte or self.empty_byte)
        _chunk_size = (chunk_size or self.chunk_size)
        binary_data = self.read_binary_file(filename, _chunk_size, _empty_byte)
        prefix, records = self.split_prefix_and_records(binary_data['adc_records'], _empty_byte)
        metadata = self.get_unit_number_and_creation_datetime(binary_data['header'])
        metadata.update(self.find_min_and_max_datetimes(records))
        print(metadata)
        return {'metadata': metadata, 'prefix': prefix, 'records': records}

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

    def get_unit_number_and_creation_datetime(self, header: bytes) -> Dict[str, Union[int, Tuple[int, int, int, int, int, int]]]:
        timestamp, unit_number = struct.unpack('<2I', header[22:30])
        creation_datetime = self.get_unit_datetime(timestamp)
        return {'creation_datetime': creation_datetime, 'unit_number': unit_number}

    def get_metadata(self, header: bytes, readings: List[bytes]) -> Dict[str, int]:
        creation_datetime = self.get_tm_datetime(header[22:26])
        unit_number = struct.unpack('<I', header[26:30])[0]
        min_and_max_datetimes = self.find_min_and_max_datetimes(readings)
        metadata = {'creation_datetime': creation_datetime, 'unit_number': unit_number}
        metadata.update(min_and_max_datetimes)
        print(metadata)
        return metadata

    def show_metadata(self, metadata: Union[Dict[str, int], Dict[str, str], Dict[str, Tuple[int, int, int, int, int, int]]]) -> None:
        print(f"\nUnit number: {metadata['unit_number']}\n")
        str_list = ['{:02d}.{:02d}.{:d}'.format(*metadata['creation_datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*metadata['creation_datetime'][3:])]
        print('Creation timestamp:\t{}'.format(' '.join(str_list)))
        self.show_min_and_max_datetimes(metadata)

    def find_min_and_max_datetimes(self, binary_data: List[bytes]) -> Dict[str, Tuple[int, int, int, int, int, int]]:
        max_datetime = b'\x00\x00\x00\x01'
        min_datetime = b'\xff\xff\xff\xff'
        for chunk in binary_data:
            timestamp = chunk[2:6][::-1]
            if timestamp > max_datetime:
                max_datetime = timestamp
            if timestamp < min_datetime:
                min_datetime = timestamp
        return {
            'min_datetime': self.get_tm_datetime(min_datetime[::-1]), 
            'max_datetime': self.get_tm_datetime(max_datetime[::-1])}


    def show_min_and_max_datetimes(self, ts: Dict[str, Tuple[int, int, int, int, int, int]]) -> None:
        str_list = ['{:02d}.{:02d}.{:d}'.format(*ts['min_datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*ts['min_datetime'][3:])]
        # str_list = ['{:02d}.{:02d}.{:d}'.format(*self.get_tm_datetime(ts['min_datetime'])[2::-1]),
        # '{:02d}:{:02d}:{:02d}'.format(*self.get_tm_datetime(ts['min_datetime'])[3:])]
        print('Minimum timestamp:\t{}'.format(' '.join(str_list)))
        str_list = ['{:02d}.{:02d}.{:d}'.format(*ts['max_datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*ts['max_datetime'][3:])]
        # str_list = ['{:02d}.{:02d}.{:d}'.format(*self.get_tm_datetime(ts['max_datetime'])[2::-1]),
        # '{:02d}:{:02d}:{:02d}'.format(*self.get_tm_datetime(ts['max_datetime'])[3:])]
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

    def get_bits_LE(self, i: int, bits_amount: int) -> List[int]:
        return [i >> j & 1 for j in range(bits_amount)]

    def convert_to_float(self, binary_data: bytes) -> float:
        return struct.unpack('!f',binary_data)[0]

    def get_unit_datetime(self, i: int) -> Tuple[int, int, int, int, int, int]:
        mask = [0b111111, 0b111111, 0b11111, 0b11111, 0b1111, 0b11111]
        shift = [0, 6, 12, 17, 22, 26]
        dt = [i>>s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        return tuple(dt[::-1])

    #! Если количество каналов 8, то ошибки и уставки помещаются в 1 байт. Не понятно, как будет выглядеть все это для другого количества каналов (4 и 16)
    # По идее, надо передавать в метод второй параметр frm = f'>{int((len(reading[9:]) - 1)/4)}fB' для чтения значений каналов, но в таком случае код
    # замедляется в полтора раза. lim2 - подозреваю, что возможно, есть значение второй уставки.
    def decrypt_reading(self, reading: bytes) -> Dict[int, Union[tuple[int, int, int, int, int, int], List[int], List[float], int]]:
        st, length, dt, lim1, lim2, err = struct.unpack('<2BI3B', reading[:9])
        dt = self.get_unit_datetime(dt)
        lim1 = self.get_bits_LE(lim1, 8)
        # lim2 = self.get_bits_LE(lim2, 8)
        err = self.get_bits_LE(err, 8)
        *values, cs = struct.unpack('>8fB', reading[9:])
        values = [el if not e else None for el, e in zip(values, err)]
        return {'datetime': dt, 'values': values, 'errors': err, 'limits': lim1, 'cs': cs}

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
        decrypted_data = [self.decrypt_reading(chunk) for chunk in binary_data]
        print('Data decrypted in {:.2f} ms.'.format(
            (perf_counter() - time_start)*1e3))
        time_start = perf_counter()
        result = sorted(decrypted_data, key=lambda d: d['datetime'])
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
            data['readings'], data['metadata']['max_datetime'])

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
    def extract_time_period(self, binary_data: List[bytes], 
        start_timestamp: Tuple[int, int, int, int, int, int], 
        end_timestamp: Tuple[int, int, int, int, int, int]) -> List[bytes]:
        sts, ets = None, None
        try:
            sts = tuple(datetime(*start_timestamp).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong start timestamp: {err}.')
        try: 
            ets = tuple(datetime(*end_timestamp).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong end timestamp: {err}.')
        if not sts or not ets:
            return []
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

    def get_int_date(self, binary_data: bytes) -> int:
        return (struct.unpack('<H', binary_data)[0] >> 1)

    def split_readings_by_dates(self, binary_data: List[bytes]) -> Dict[str, list]:
        if not binary_data:
            return {}
        result = {}
        for chunk in binary_data:
            int_date = self.get_int_date(chunk[4:6])
            if int_date in result:
                result[int_date].append(chunk)
            else:
                result[int_date] = [chunk]
        return result

    def create_filename(self, unit_number: int, 
        start_timestamp: Tuple[int, int, int, int, int, int], 
        end_timestamp: Tuple[int, int, int, int, int, int],
        frm=None) -> str:
        dt_format = (frm or self.datetime_format) 
        sts = dt_format.format(*start_timestamp)
        ets = dt_format.format(*end_timestamp)
        return '{}_{}-{}.csv'.format(unit_number, sts, ets)


    def if_write_file(self, processed_data: List[dict], unit_number: int) -> None:
        output_filename = self.create_filename(unit_number, processed_data[0]['datetime'], processed_data[-1]['datetime'])
        str_data = [self.values_to_str(el, ';') for el in processed_data]
        self.write_file(str_data, output_filename)

    def extract_last_date_from_outside(self, raw_data: dict, write_to_file: bool =False) -> List[dict]:
        data = self.extract_last_date(raw_data)
        processed_data = self.process_chunks(data)
        if write_to_file:
            self.if_write_file(processed_data, raw_data['metadata']['unit_number'])
        return processed_data

    def extract_time_period_from_outside(self, raw_data: dict, start_timestamp: Tuple[int, int, int, int, int, int], end_timestamp: Tuple[int, int, int, int, int, int], write_to_file:bool =False):
        data = self.extract_time_period(raw_data['readings'], start_timestamp, end_timestamp)
        processed_data = self.process_chunks(data)
        if write_to_file:
            self.if_write_file(processed_data, raw_data['metadata']['unit_number'])
        return processed_data


if __name__ == '__main__':
    
    filename = 'TM100514_B.AR4'
    write_to_file = True
    config = {
        'chunk_size': 256,
        'empty_byte': b'\xff',
        'channels_amount': 8,
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
    # dates = ar4_parser.split_readings_by_dates(raw_data['readings'])
    # s = 0
    # for k, v in dates.items():
    #     for value in v:
    #         a = ar4_parser.decrypt_reading(v[0])
    #     s += len(v)
    # print(s, len(raw_data['readings']))

    chunks = ar4_parser.extract_time_period(raw_data['records'], (2023, 1, 1, 0, 0, 0), (2024, 1, 1, 0, 0, 0))

    # time_start = perf_counter()
    # processed_data_1 = [ar4_parser.decrypt_data(el) for el in chunks]
    # print('data decrypted in {:.2f} ms: {} lines.'.format((perf_counter() - time_start)*1e3, len(processed_data_1)))

    # time_start = perf_counter()
    # processed_data_2 = [ar4_parser.decrypt_reading(el) for el in chunks]
    # print('readings decrypted in {:.2f} ms: {} lines.'.format((perf_counter() - time_start)*1e3, len(processed_data_2)))
    # print(processed_data_1 == processed_data_2)
    # s = bytes.fromhex('a52a749a5a5d010080447a04334479527c447553404465d0bb4452e6c143de39db42e3674101e36741b6')
    # print(ar4_parser.decrypt_data(s))
    # print(ar4_parser.decrypt_reading(s))
    # time_start = perf_counter()
    # processed_data = ar4_parser.extract_last_date_from_outside(raw_data, write_to_file=True)
    # processed_data = ar4_parser.extract_time_period_from_outside(raw_data, start_timestamp, end_timestamp, write_to_file=True)
    # print('Last date exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(processed_data)))
    
    # chunks = ar4_parser.read_in_chunks(filename, 256)

    # s = 'a52a67ec485e0000f843dac32943efe6ce43f6125201f6125201f6125201f6125201f6125201f61252df'
    # n = 101
    # one_reading = bytes.fromhex(s)

    # print(*struct.unpack('<2BI3B', one_reading[:9]))
    # print(*struct.unpack('>8fB', one_reading[9:]))
    # d1 = ar4_parser.decrypt_reading_2(one_reading)
    # d2 = ar4_parser.decrypt_data(one_reading)
    # print(d1 == d2)

    # print(*[el.hex() for el in struct.unpack('ss4ssss4s4s4s4s4s4s4s4ss', one_reading)])
    # print(*[el.hex() for el in struct.unpack('s s 4s s s s 4s 4s 4s 4s 4s 4s 4s 4s s', one_reading)])
    # print(*[hex(el) for el in [165, 42, 24136, 248, 67]])
    # print(*[el for el in struct.unpack('<B B I B B B f f f f f f f f B', one_reading)])
    # print(*[el for el in struct.unpack('>B B I B B B f f f f f f f f B', one_reading)])
    # print(*[el for el in struct.unpack('BBIBBfffffffBs', one_reading)])

    # print(ar4_parser.get_bits(n, 8))
    # print(ar4_parser.get_bits_LE(n, 8)[::-1])


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

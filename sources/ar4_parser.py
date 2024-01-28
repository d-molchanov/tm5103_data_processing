from datetime import datetime, timedelta
from time import perf_counter
import struct

class Ar4Parser():

    def __init__(self):
        self.chunk_size = 256
        self.empty_byte = b'\xff'

    def read_in_chunks(self, filename: str, chunk_size: int) -> list:
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
    def cut_off_empty_tail(self, chunks:list, chunk_size:int, empty_byte:str) -> list:
        if not chunks:
            return []

        chunk_with_no_data = chunk_size*empty_byte
        for i, ch in enumerate(chunks[::-1]):
            if ch != chunk_with_no_data:
                return chunks[:len(chunks) - i]

    def extract_data_chunks(self, binary_data: list, empty_byte: bytes) -> list:
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


    def read_binary_file(self, filename: str, chunk_size: int,  empty_byte: bytes) -> dict:
    
        big_chunks = self.read_in_chunks(filename, chunk_size)
        if not big_chunks:
            return {'header': None, 'data': []}

        data_chunks = self.cut_off_empty_tail(big_chunks, chunk_size, empty_byte)
        reduced_chunks = []
        for chunk in data_chunks[1:]:
            reduced_chunks.extend(self.extract_data_chunks(chunk, empty_byte))
        
        return {'header': big_chunks[0], 'data': reduced_chunks}

    def split_prefix_and_reading(self, binary_data: list, empty_byte: bytes) -> list:
        if not binary_data:
            return []
        not_datetime = 4*empty_byte

        split_index = 0
        for i, line in enumerate(binary_data):
            if line[2:6] != not_datetime:
                split_index = i
                break
        
        return {'prefix': binary_data[:split_index], 
                'readings': binary_data[split_index:]}

    def parse_ar4_file(self, filename: str, chunk_size=None, empty_byte=None) -> dict:
        if empty_byte == None:
            empty_byte = self.empty_byte
        if chunk_size == None:
            chunk_size = self.chunk_size

        binary_data = self.read_binary_file(filename, chunk_size, empty_byte)
        processed_data = self.split_prefix_and_reading(binary_data['data'], empty_byte)
        # binary_data = bd['data']
        prefix = processed_data['prefix']
        readings = processed_data['readings']
        metadata = self.get_metadata(binary_data['header'], readings)
        return {'metadata': metadata, 'prefix': prefix, 'readings': readings}

    def get_tm_datetime(self, binary_data: bytes):
        dt_int = struct.unpack('<I', binary_data)[0]
        mask = [0b111111, 0b111111, 0b11111, 0b11111, 0b1111, 0b11111]
        shift = [0, 6, 12, 17, 22, 26]
        dt = [dt_int>>s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        # return datetime(*dt[::-1])
        return tuple(dt[::-1])

    def get_metadata(self, header: list, readings: list) -> dict:
        creation_datetime = self.get_tm_datetime(header[22:26])
        unit_number = struct.unpack('<I', header[26:30])[0]
        min_and_max_timestamps = self.find_min_and_max_timestamps(readings)
        metadata = {'creation_datetime': creation_datetime, 'unit_number': unit_number}
        metadata.update(min_and_max_timestamps)
        print(metadata)
        return metadata

    def show_metadata(self, metadata: dict) -> None:
        print(f"\nUnit number: {metadata['unit_number']}\n")
        str_list = ['{:02d}.{:02d}.{:d}'.format(*metadata['creation_datetime'][2::-1]),
        '{:02d}:{:02d}:{:02d}'.format(*metadata['creation_datetime'][3:])]
        print('Creation timestamp:\t{}'.format(' '.join(str_list)))
        self.show_min_and_max_timestamps(metadata)

    def find_min_and_max_timestamps(self, binary_data: list) -> list:
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


    def show_min_and_max_timestamps(self, ts: dict) -> None:
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

    def extract_one_date(self, binary_data: list, timestamp: tuple) -> list:
        result = []
        year, month, day = timestamp[:3]
        date = ((day-1)) + ((month-1)<<5) + ((year - 2000)<<9)
        for chunk in binary_data:
            dt = struct.unpack('<H', chunk[4:6])[0] >> 1
            if dt == date:
                result.append(chunk)
        return result

    #Является ли binary_data строкой? По идее - да, это байтовая строка
    def get_bits(self, binary_data: str, n: int) -> list:
        result = []
        for i in range(n):
            result.append(binary_data >> i & 1)
        return result[::-1]

    #Является ли binary_data строкой? По идее - да, это байтовая строка
    def convert_to_float(self, binary_data: str) -> list:
        return struct.unpack('!f',binary_data)[0]

    def decrypt_data(self, binary_data: bytes) -> dict:
        indices = [0, 2, 6, 7, 8, 9, 13, 17, 21, 25, 29, 33, 37, 41, 42]
        temp_data = [binary_data[i1:i2] for i1, i2 in zip(indices[:-1], indices[1:])]
        dt = self.get_tm_datetime(temp_data[1])
        limits = self.get_bits(ord(temp_data[2]), 8)[::-1]

        err = self.get_bits(ord(temp_data[4]), 8)[::-1]
        values = [self.convert_to_float(el) if not e else None for el, e in zip(temp_data[5:-1], err)]
        cs = binary_data[-1]
        return {'datetime': dt, 'values': values, 'errors': err, 'limits': limits, 'cs': cs}

    def process_chunks(self, binary_data: list) -> list:
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
        # str_data = ['{:02d}.{:02d}.{:d}'.format(*reading['datetime'][2::-1]),
        str_data = ['{:02d}:{:02d}:{:02d}'.format(*reading['datetime'][3:]),
        *['{:.6f}'.format(v).replace('.', ',') if v != None else 'None' for v in reading['values']]]
        return '{}\n'.format(sep.join(str_data))

    def write_file(self, data: list, filename: str) -> None:
        try:
            with open(filename, 'w') as f:
                for line in data:
                    f.write(line)
        except IOError:
            print(f'Error with <{filename}>.')

    def extract_last_date(self, filename: str, chunk_size=None, empty_byte=None) -> None:
        if empty_byte == None:
            empty_byte = self.empty_byte
        if chunk_size == None:
            chunk_size = self.chunk_size

        d = self.parse_ar4_file(filename, chunk_size, empty_byte)
        self.show_metadata(d['metadata'])


        last_date = self.extract_one_date(d['readings'], d['metadata']['max_timestamp'])

        data = self.process_chunks(last_date)
        return data
        # self.write_file(str_data, '{}.csv'.format(datetime(*self.get_tm_datetime(ts['max_timestamp'])).strftime('%Y_%m_%d')))

    def extract_last_date_new(self, data: dict) -> list:
        last_date = self.extract_one_date(
            data['readings'], data['metadata']['max_timestamp'])
        # return self.process_chunks(last_date)
        return last_date

    def convert_timestamp_to_int(self, timestamp: tuple) -> int:
        return (
            (timestamp[5]) +
            (timestamp[4]<<6) +
            (timestamp[3]<<12) +
            ((timestamp[2]-1)<<17) + 
            ((timestamp[1]-1)<<22) + 
            ((timestamp[0] - 2000)<<26)
        )

    def extract_time_period(self, filename: str, start_timestamp: tuple, end_timestamp: tuple, chunk_size=None, empty_byte=None) -> None:
        if empty_byte == None:
            empty_byte = self.empty_byte
        if chunk_size == None:
            chunk_size = self.chunk_size

        if len(start_timestamp) == 3:
            start_timestamp += (0, 0, 0)
        if len(end_timestamp) == 3:
            end_timestamp += (23, 59, 59)
        start_ts = self.convert_timestamp_to_int(start_timestamp)
        end_ts = self.convert_timestamp_to_int(end_timestamp)
        d = self.parse_ar4_file(filename, chunk_size, empty_byte)
        self.show_metadata(d['metadata'])
        data = []
        for chunk in d['readings']:
            ts = struct.unpack('<I', chunk[2:6])[0]
            if start_ts <= ts <= end_ts:
                data.append(chunk)
        processed_data = self.process_chunks(data)
        return processed_data

    def extract_time_period_new(self, data: dict, start_timestamp: tuple, end_timestamp: tuple) -> list:
        try:
            sts = tuple(datetime(*start_timestamp).timetuple())[:6]
            # ets = tuple((datetime(*end_timestamp) + timedelta(days=1)).timetuple())[:6]
            ets = tuple(datetime(*end_timestamp).timetuple())[:6]
        except ValueError as err:
            print(err)
        # print(sts, ets, sep='\n')
        # print(struct.pack('<I', self.convert_timestamp_to_int(ets)).hex())
        # print(data['readings'][-1][2:6].hex())
        start_ts = self.convert_timestamp_to_int(sts)
        end_ts = self.convert_timestamp_to_int(ets)
        d = []
        for chunk in data['readings']:
            ts = struct.unpack('<I', chunk[2:6])[0]
            if start_ts <= ts < end_ts:
                d.append(chunk)
        # return self.process_chunks(d)
        return d

    def extract_time_period_new_2(self, data: dict, start_timestamp: tuple, end_timestamp: tuple) -> list:
        try:
            sts = tuple(datetime(*start_timestamp).timetuple())[:6]
            # ets = tuple((datetime(*end_timestamp) + timedelta(days=1)).timetuple())[:6]
            ets = tuple(datetime(*end_timestamp).timetuple())[:6]
        except ValueError as err:
            print(err)
        start_ts = struct.pack('>I', self.convert_timestamp_to_int(sts))
        end_ts = struct.pack('>I', self.convert_timestamp_to_int(ets))

        d = []
        for chunk in data['readings']:
            # ts = struct.unpack('<I', chunk[2:6])[0]
            # if start_ts <= chunk[2:6][::-1] < end_ts:
            if start_ts <= chunk[5:1:-1] < end_ts:
                d.append(chunk)
        # return self.process_chunks(d)
        return d
        

if __name__ == '__main__':
    
    filename = 'TM100514_B.AR4'
    write_to_file = False
    output_filename = 'out2.csv'
    start_timestamp = (2023, 1, 5)
    # start_timestamp = (2023, 10, 5)
    end_timestamp = (2023, 10, 6)
    # end_timestamp = (2023, 10, 5, 14, 49, 44)

    ar4_parser = Ar4Parser()
    raw_data = ar4_parser.parse_ar4_file(filename)
    time_start = perf_counter()
    data1 = ar4_parser.extract_last_date_new(raw_data)
    print('Last date exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(data1)))
    time_start = perf_counter()
    data2 = ar4_parser.extract_time_period_new(raw_data, start_timestamp, end_timestamp)
    print('Time period exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(data2)))
    time_start = perf_counter()
    data3 = ar4_parser.extract_time_period_new_2(raw_data, start_timestamp, end_timestamp)
    print('Time period exctracted in {:.2f} ms. {} rows.'.format((perf_counter() - time_start)*1e3, len(data3)))
    print(data2 == data3)
    # data = ar4_parser.extract_last_date(filename)
    # ar4_parser.extract_last_date(filename, ar4_parser.chunk_size, ar4_parser.empty_byte)
    # data = ar4_parser.extract_time_period(filename, start_timestamp, end_timestamp)
    if write_to_file:
        str_data = [ar4_parser.values_to_str(el, ';') for el in data]
        ar4_parser.write_file(str_data, output_filename)

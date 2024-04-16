import os
import struct
import json
import csv
from typing import List, Tuple, Dict, Optional, Union
from time import perf_counter
from datetime import datetime, timedelta

Unit_datetime = Tuple[int, int, int, int, int, int]


class Ar4Parser():

    def __init__(self):
        self.chunk_size = 256
        self.empty_byte = b'\xff'
        self.channels_amount = 8
        self.file_sep = ';'
        self.datetime_format = '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}'
        self.file_ext = 'csv'

    def find_config_file(self, target_dir: str) -> None:
        pass

    def export_config(self, dir_to_export: Union[str, None] = None):
        _dir_to_export = (dir_to_export or '.')
        config = {
            'chunk_size': self.chunk_size,
            'empty_byte': self.empty_byte.hex(),
            'channels_amount': self.channels_amount,
            'file_sep': self.file_sep,
            'datetime_format': self.datetime_format,
            'file_ext': self.file_ext
        }
        abs_path = os.path.abspath(_dir_to_export)
        filename = os.path.join(abs_path, 'ar4_parser_config.json')
        try:
            with open(filename, 'w') as f:
                json.dump(config, f, indent=4)
            print(f'Config is saved at {filename}')
        except IOError as err:
            print(err)

    def read_config(self, filename: str) -> dict:
        abs_path = os.path.abspath(filename)
        config = {}
        try:
            with open(filename, 'r') as f:
                config = json.load(f)
            print(f'Config was read from {abs_path}')
        except IOError:
            print(f'Unable to find {abs_path}. Check, that filename of config file is correct.')
        if 'empty_byte' in config:
            config['empty_byte'] = bytes.fromhex(config['empty_byte'])
        return config

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
    def cut_off_empty_tail(self, chunks: List[bytes], chunk_size: int, empty_byte: bytes) -> List[bytes]:
        result: List[bytes] = []
        if not chunks:
            return result

        chunk_with_no_data = chunk_size*empty_byte
        for i, ch in enumerate(chunks[::-1]):
            if ch != chunk_with_no_data:
                result = chunks[:len(chunks) - i]
                break
        return result

    def extract_records(
        self, binary_data: bytes, empty_byte: bytes
    ) -> List[bytes]:
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

    def read_binary_file(
        self,
        filename: str,
        chunk_size: int, 
        empty_byte: bytes
    ) -> Dict[str, Union[List[bytes], bytes, None]]:

        big_chunks = self.read_in_chunks(filename, chunk_size)
        if not big_chunks:
            return {'header': None, 'adc_records': []}

        data_chunks = self.cut_off_empty_tail(big_chunks, chunk_size, empty_byte)
        records = []
        for chunk in data_chunks[1:]:
            records.extend(self.extract_records(chunk, empty_byte))

        return {'header': big_chunks[0], 'adc_records': records}

    # Maybe change 'prefix' to 'overhead'
    def split_prefix_and_records(
        self, adc_records: List[bytes], empty_byte: bytes
    ) -> Dict[str, List[bytes]]:
        if not adc_records:
            return {}
        not_datetime = 4*empty_byte

        split_index = 0
        for i, line in enumerate(adc_records):
            if line[2:6] != not_datetime:
                split_index = i
                break

        return (adc_records[:split_index], adc_records[split_index:])

    def get_unit_datetime(self, i: int) -> Unit_datetime:
        mask = [0b111111, 0b111111, 0b11111, 0b11111, 0b1111, 0b11111]
        shift = [0, 6, 12, 17, 22, 26]
        dt = [i >> s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        return tuple(dt[::-1])

    def get_unit_number_and_creation_datetime(
        self,
        header: bytes
    ) -> Dict[str, Union[int, Unit_datetime]]:
        timestamp, unit_number = struct.unpack('<2I', header[22:30])
        creation_datetime = self.get_unit_datetime(timestamp)

        return {
            'creation_datetime': creation_datetime, 
            'unit_number': unit_number}

    def find_min_and_max_datetimes(
        self, binary_data: List[bytes]
    ) -> Dict[str, Tuple[int, int, int, int, int, int]]:
        max_datetime = b'\x00\x00\x00\x01'
        min_datetime = b'\xff\xff\xff\xff'
        for chunk in binary_data:
            timestamp = chunk[2:6][::-1]
            if timestamp > max_datetime:
                max_datetime = timestamp
            if timestamp < min_datetime:
                min_datetime = timestamp
        min_dt = struct.unpack('>I', min_datetime)[0]
        max_dt = struct.unpack('>I', max_datetime)[0]
        return {
            'min_datetime': self.get_unit_datetime(min_dt), 
            'max_datetime': self.get_unit_datetime(max_dt)}

    def parse_ar4_file(
        self,
        filename: str,
        chunk_size: Optional[int] = None,
        empty_byte: Optional[bytes] = None
    ) -> Union[Dict[str, List[bytes]], Dict[str, int]]:
        _empty_byte = (empty_byte or self.empty_byte)
        _chunk_size = (chunk_size or self.chunk_size)
        binary_data = self.read_binary_file(
            filename, _chunk_size, _empty_byte)
        prefix, records = self.split_prefix_and_records(
            binary_data['adc_records'], _empty_byte)
        metadata = self.get_unit_number_and_creation_datetime(
            binary_data['header'])
        metadata.update(self.find_min_and_max_datetimes(records))
        self.show_metadata(metadata)
        return {'metadata': metadata, 'prefix': prefix, 'records': records}

    def show_datetime(self, title: str, unit_datetime: Unit_datetime) -> None:
        print('{0}\t{3:02d}.{2:02d}.{1:d} {4:02d}:{5:02d}:{6:02d}'.format(
            title, *unit_datetime))
        return None

    def show_metadata(self, metadata):
        print(f"\nUnit number: {metadata['unit_number']}\n")
        self.show_datetime('Creation datetime:', metadata['creation_datetime'])
        self.show_datetime('Minimum datetime: ', metadata['min_datetime'])
        self.show_datetime('Maximum datetime: ', metadata['max_datetime'])
        return None

    def convert_unit_datetime_to_int(
        self, unit_datetime: Unit_datetime
    ) -> int:
        return (
            (unit_datetime[5]) +
            (unit_datetime[4] << 6) +
            (unit_datetime[3] << 12) +
            ((unit_datetime[2]-1) << 17) + 
            ((unit_datetime[1]-1) << 22) + 
            ((unit_datetime[0] - 2000) << 26)
        )

    # Посмотреть перевод "начало временного интервала"
    def extract_time_period(
        self,
        records: List[bytes],
        start_datetime: Unit_datetime,
        end_datetime: Unit_datetime
    ) -> List[bytes]:
        sdt, edt = None, None
        try:
            sdt = tuple(datetime(*start_datetime).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong start timestamp: {err}.')
        try: 
            edt = tuple(datetime(*end_datetime).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong end timestamp: {err}.')
        if not sdt or not edt:
            return []
        start_ts = struct.pack('>I', 
            self.convert_unit_datetime_to_int(sdt)
        )
        end_ts = struct.pack('>I', 
            self.convert_unit_datetime_to_int(edt)
        )

        result = []
        for record in records:
            if start_ts <= record[5:1:-1] < end_ts:
                result.append(record)
        return result

    def extract_one_date(self, records: List[bytes],
        unit_datetime: Unit_datetime) -> List[bytes]:
        start_datetime = unit_datetime[:3] + (0, 0, 0)
        try:
            end_datetime = tuple((
                    datetime(*start_datetime)+
                    timedelta(days=1)).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong datetime: {err}')
            return []

        return self.extract_time_period(
            records, start_datetime, end_datetime)

    def extract_last_date(self, raw_data: dict) -> List[bytes]:
        return self.extract_one_date(
            raw_data['records'], raw_data['metadata']['max_datetime'])

    def get_int_date(self, binary_data: bytes) -> int:
        return (struct.unpack('<H', binary_data)[0] >> 1)

    def split_records_by_dates(self, records: List[bytes]) -> Dict[str, list]:
        if not records:
            return {}
        result = {}
        for record in records:
            int_date = self.get_int_date(record[4:6])
            if int_date in result:
                result[int_date].append(record)
            else:
                result[int_date] = [record]
        return result

    def get_bits_LE(self, i: int, bits_amount: int) -> List[int]:
        return [i >> j & 1 for j in range(bits_amount)]

    #! Если количество каналов 8, то ошибки и уставки помещаются в 1 байт. Не понятно, как будет выглядеть все это для другого количества каналов (4 и 16)
    # По идее, надо передавать в метод второй параметр frm = f'>{int((len(reading[9:]) - 1)/4)}fB' для чтения значений каналов, но в таком случае код
    # замедляется в полтора раза. lim2 - подозреваю, что возможно, есть значение второй уставки.
    def decrypt_record(self, record: bytes) -> Dict[
        int, Union[Unit_datetime, List[int], List[float], int]]:

        _, length, int_dt, lim1, lim2, err = struct.unpack(
            '<2BI3B', record[:9])
        dt = self.get_unit_datetime(int_dt)
        lim1 = self.get_bits_LE(lim1, 8)
        # lim2 = self.get_bits_LE(lim2, 8)
        err = self.get_bits_LE(err, 8)
        *readings, cs = struct.unpack('>8fB', record[9:])
        readings = [el if not e else None for el, e in zip(readings, err)]

        return {
            'datetime': dt, 'readings': readings,
            'errors': err, 'limits': lim1, 'cs': cs}

    def decrypt_records(self, records: List[bytes]) -> List[dict]:
        if not records:
            return []
        time_start = perf_counter()
        decrypted_records = [
        self.decrypt_record(record) for record in records]
        print('{} records decrypted in {:.2f} ms.'.format(
            len(decrypted_records),
            (perf_counter() - time_start)*1e3))
        time_start = perf_counter()
        result = sorted(decrypted_records, key=lambda d: d['datetime'])
        print('{} records sorted in {:.2f} ms.'.format(
            len(result),
            (perf_counter() - time_start)*1e3))
        return result

    def convert_decrypted_record_to_str(self, record: dict, sep: str) -> str:
        return sep.join(
            [
                '{2:02d}.{1:02d}.{0:d}{6}{3:02d}:{4:02d}:{5:02d}'.format(
                *record['datetime'], sep),
                *['{:.6f}'.format(r).replace('.', ',') if r != None else
                 'None' for r in record['readings']]
            ]
        )

    def write_decrypted_records_to_file(
        self,
        decrypted_records: List[dict],
        filename: str, sep: str
    ) -> None:
        try:
            with open(filename, 'w') as f:
                for decrypted_record in decrypted_records:
                    f.write(f'{self.convert_decrypted_record_to_str(decrypted_record, sep)}\n')
        except IOError:
            print(f'Error with <{filename}>.')
        return None

    def create_filename(
        self, unit_number: int,
        start_datetime: Unit_datetime, end_datetime: Unit_datetime,
        frm=None, file_ext=None
    ) -> str:

        dt_format = (frm or self.datetime_format)
        _file_ext = (file_ext or self.file_ext)
        sdt = dt_format.format(*start_datetime)
        edt = dt_format.format(*end_datetime)

        return '{}_{}-{}.{}'.format(unit_number, sdt, edt, _file_ext)

    def export_decrypted_records_to_file(
        self,
        records: List[dict],
        unit_number: int, sep: str
    ) -> None:
        filename = self.create_filename(
            unit_number,
            records[0]['datetime'],
            records[-1]['datetime']
        )
        self.write_decrypted_records_to_file(records, filename, sep)
        return None

    def extract_last_date_from_outside(
        self, raw_data: dict, sep = None, write_to_file: bool = False
    ) -> List[dict]:

        file_sep = (sep or self.file_sep)
        records = self.extract_last_date(raw_data)
        decrypted_records = self.decrypt_records(records)
        if write_to_file:
            self.export_decrypted_records_to_file(
                decrypted_records,
                raw_data['metadata']['unit_number'],
                file_sep
            )
        return decrypted_records

    def extract_time_period_from_outside(
        self,
        raw_data: dict,
        start_datetime: Unit_datetime,
        end_datetime: Unit_datetime,
        sep = None,
        write_to_file: bool = False
    ):

        file_sep = (sep or self.file_sep)
        records = self.extract_time_period(
            raw_data['records'], start_datetime, end_datetime)
        decrypted_records = self.decrypt_records(records)
        if write_to_file:
            self.export_decrypted_records_to_file(
                decrypted_records, raw_data['metadata']['unit_number'], file_sep)
        return decrypted_records

    # def convert_int_to_unit_date(self, int_date: int) -> Tuple[int, int, int, int, int, int]:
    #     mask = [0b11111, 0b1111, 0b11111]
    #     shift = [0, 5, 9]
    #     dt = [int_date>>s & m for s, m in zip(shift, mask)]
    #     dt[-1] += 2000
    #     dt[-2] += 1
    #     dt[-3] += 1
    #     return tuple(dt[::-1])


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
    start_datetime = (2023, 10, 5)
    end_datetime = (2023, 10, 6)

    ar4_parser = Ar4Parser()
    # ar4_parser.export_config()
    ar4_parser.read_config('ar4_parser_config.json')
    ar4_parser.config_parser(config)
    raw_data = ar4_parser.parse_ar4_file(filename)

    data_1 = ar4_parser.extract_time_period(
        raw_data['records'], start_datetime, end_datetime)
    data_2 = ar4_parser.extract_one_date(raw_data['records'], start_datetime)
    data_3 = ar4_parser.extract_last_date(raw_data)
    d_4 = ar4_parser.split_records_by_dates(raw_data['records'])
    k = max(list(d_4.keys()))
    data_4 = d_4[k]
    print(data_1 == data_2, data_2 == data_3, data_3 == data_4, data_4 == data_1)
    print(len(data_1), len(data_2), len(data_3), len(data_4))

    data_5 = ar4_parser.extract_time_period_from_outside(
        raw_data, start_datetime, end_datetime, write_to_file=True)
    data_6 = ar4_parser.extract_last_date_from_outside(
        raw_data, write_to_file=True)

    print(data_5 == data_6, len(data_5), len(data_6))

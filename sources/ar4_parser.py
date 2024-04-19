"""Модуль, содержащий класс Ar4Parser, предназначенный для
   декодирования файлов формата .AR4
"""
import os
import struct
import json
import csv
from typing import List, Tuple, Dict, Optional, Union
from time import perf_counter
from datetime import datetime, timedelta

UnitDatetime = Tuple[int, int, int, int, int, int]


class Ar4Parser():
    """Класс для декодирования архива с расширением .AR4,
       извлеченного из внутренний памяти аналогово-цифрового
       преобразователя <unit_name> компании <company_name>.

       Атрибуты:
       __chunk_size (int): Размер блока данных для чтения из файла (по умолчанию 256 байт).
       __empty_byte (bytes): Байтовое представление пустого значения (по умолчанию ).
       __channels_amount (int): Количество каналов данных в АЦП (по умолчанию 8).
       __file_sep (str): Разделитель полей в выходном CSV файле (по умолчанию ';').
       __datetime_format (str): Формат строки даты и времени (по умолчанию '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}').
       __file_ext (str): Расширение выходного файла (по умолчанию 'csv')

    """

    def __init__(self):
        self.__chunk_size = 256
        self.__empty_byte = b'\xff'
        self.__channels_amount = 8
        self.__file_sep = ';'
        self.__datetime_format = '{:d}{:02d}{:02d}{:02d}{:02d}{:02d}'
        self.__file_ext = 'csv'

    def find_config_file(self, target_dir: str) -> None:
        pass

    def export_config(self, dir_to_export: Union[str, None] = None):
        """Метод для экспорта конфигурации класса в JSON-файл.

        """
        _dir_to_export = (dir_to_export or '.')
        config = {
            'chunk_size': self.__chunk_size,
            'empty_byte': self.__empty_byte.hex(),
            'channels_amount': self.__channels_amount,
            'file_sep': self.__file_sep,
            'datetime_format': self.__datetime_format,
            'file_ext': self.__file_ext
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
        """Метод для чтения JSON-файла, содержащего параметры
           экземпляра класса Ar4Parser.
        """
        abs_path = os.path.abspath(filename)
        config = {}
        try:
            with open(filename, 'r') as f:
                config = json.load(f)
            print(f'Config was read from {abs_path}')
        except IOError:
            print(f'Unable to find {abs_path}.')
        if 'empty_byte' in config:
            config['empty_byte'] = bytes.fromhex(config['empty_byte'])
        return config

    def config_parser(self, config: Dict[str, Union[str, int]]) -> None:
        """Метод для конфигурации экземпляра класса Ar4Parser.
        """
        if 'chunk_size' in config:
            self.__chunk_size = config['chunk_size']
        if 'empty_byte' in config:
            self.__empty_byte = config['empty_byte']
        if 'channels_amount' in config:
            self.__channels_amount = config['channels_amount']
        if 'file_sep' in config:
            self.__file_sep = config['file_sep']
        if 'datetime_format' in config:
            self.__datetime_format = config['datetime_format']
        if 'file_ext' in config:
            self.__file_ext = config['file_ext']

    def read_in_chunks(self, filename: str, chunk_size: int) -> List[bytes]:
        """Метод для чтения бинарного файла по частям
        """
        time_start = perf_counter()
        result = []
        try:
            with open(filename, 'rb') as f:
                chunk = f.read(chunk_size)
                while chunk:
                    result.append(chunk)
                    chunk = f.read(chunk_size)
                print(
                    f'Read ``{filename}`` in',
                    f'{(perf_counter() - time_start)*1e3:.2f} ms.'
                )
        except IOError as err:
            print(f'Error with <{filename}>:\n{err}.')
        return result

        # Check working with full archive!
    def cut_off_empty_tail(
        self, chunks: List[bytes], chunk_size: int, empty_byte: bytes
    ) -> List[bytes]:
        """Метод для отсечения незаполненного полезной информацией
           окончания архива

        """
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
        """Метод для разбиения бинарной строки на записи
        """
        result = []
        i = 0
        while i < len(binary_data):
            if binary_data[i] != empty_byte[0]:
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
        """Метод для чтения бинарного файла"""
        big_chunks = self.read_in_chunks(filename, chunk_size)
        if not big_chunks:
            return {'header': None, 'adc_records': []}

        data_chunks = self.cut_off_empty_tail(
            big_chunks, chunk_size, empty_byte
        )
        records = []
        for chunk in data_chunks[1:]:
            records.extend(self.extract_records(chunk, empty_byte))

        return {'header': big_chunks[0], 'adc_records': records}

    # Maybe change 'prefix' to 'overhead'
    def split_prefix_and_records(
        self, adc_records: List[bytes], empty_byte: bytes
    ) -> Dict[str, List[bytes]]:
        """Метод для разбиения метаданных и записей"""
        if not adc_records:
            return {}
        not_datetime = 4*empty_byte

        split_index = 0
        for i, line in enumerate(adc_records):
            if line[2:6] != not_datetime:
                split_index = i
                break

        return (adc_records[:split_index], adc_records[split_index:])

    def __get_unit_datetime(self, i: int) -> UnitDatetime:
        """Метод для получения времени и даты в формате <unit_name>"""
        mask = [0b111111, 0b111111, 0b11111, 0b11111, 0b1111, 0b11111]
        shift = [0, 6, 12, 17, 22, 26]
        dt = [i >> s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        return tuple(dt[::-1])

    def __get_unit_number_and_creation_datetime(
        self,
        header: bytes
    ) -> Dict[str, Union[int, UnitDatetime]]:
        """Метод для извлечения серийного номера прибора и
           даты создания архива
        """
        timestamp, unit_number = struct.unpack('<2I', header[22:30])
        creation_datetime = self.__get_unit_datetime(timestamp)

        return {
            'creation_datetime': creation_datetime,
            'unit_number': unit_number}

    def __find_min_and_max_datetimes(
        self, binary_data: List[bytes]
    ) -> Dict[str, UnitDatetime]:
        """Метод для поиска наименьшей и наибольшей даты в архиве
        """
        max_datetime = b'\x00\x00\x00\x01'
        min_datetime = b'\xff\xff\xff\xff'
        for chunk in binary_data:
            timestamp = chunk[2:6][::-1]
            # Такой код замедляет выполнение программы на ~20%
            # min_datetime = min(timestamp, min_datetime)
            # max_datetime = max(timestamp, max_datetime)
            if timestamp > max_datetime:
                max_datetime = timestamp
            if timestamp < min_datetime:
                min_datetime = timestamp
        min_dt = struct.unpack('>I', min_datetime)[0]
        max_dt = struct.unpack('>I', max_datetime)[0]
        return {
            'min_datetime': self.__get_unit_datetime(min_dt),
            'max_datetime': self.__get_unit_datetime(max_dt)}

    def parse_ar4_file(
        self,
        filename: str,
        chunk_size: Optional[int] = None,
        empty_byte: Optional[bytes] = None
    ) -> Union[Dict[str, List[bytes]], Dict[str, int]]:
        """Метод для извлечения информации из архива
        """
        _empty_byte = (empty_byte or self.__empty_byte)
        _chunk_size = (chunk_size or self.__chunk_size)
        binary_data = self.read_binary_file(
            filename, _chunk_size, _empty_byte)
        prefix, records = self.split_prefix_and_records(
            binary_data['adc_records'], _empty_byte)
        metadata = self.__get_unit_number_and_creation_datetime(
            binary_data['header'])
        metadata.update(self.__find_min_and_max_datetimes(records))
        self.__show_metadata(metadata)
        return {'metadata': metadata, 'prefix': prefix, 'records': records}

    def __show_datetime(self, title: str, unit_datetime: UnitDatetime) -> None:
        """Метод для вывода в консоль даты и времени
        """
        print('{0}\t{3:02d}.{2:02d}.{1:d} {4:02d}:{5:02d}:{6:02d}'.format(
            title, *unit_datetime))

    def __show_metadata(self, metadata):
        """Метод для вывода в консоль метаданных архива
        """
        print(f"\nUnit number: {metadata['unit_number']}\n")
        self.__show_datetime(
            'Creation datetime:', metadata['creation_datetime']
        )
        self.__show_datetime('Minimum datetime: ', metadata['min_datetime'])
        self.__show_datetime('Maximum datetime: ', metadata['max_datetime'])

    def __convert_unit_datetime_to_int(
        self, unit_datetime: UnitDatetime
    ) -> int:
        """Метод для преобразования времени и даты из формата
           <unit_datetime> в целое число
        """
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
        start_datetime: UnitDatetime,
        end_datetime: UnitDatetime
    ) -> List[bytes]:
        """Метод для извлечения из архива заданного временного интервала
        """
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
        start_ts = struct.pack(
            '>I', self.__convert_unit_datetime_to_int(sdt)
        )
        end_ts = struct.pack(
            '>I', self.__convert_unit_datetime_to_int(edt)
        )

        result = []
        for record in records:
            if start_ts <= record[5:1:-1] < end_ts:
                result.append(record)
        return result

    def extract_one_date(
        self, records: List[bytes], unit_datetime: UnitDatetime
    ) -> List[bytes]:
        """Метод для извлечения из архива записей, соответствующих
           заданной дате.
        """
        start_datetime = unit_datetime[:3] + (0, 0, 0)
        try:
            end_datetime = tuple((
                    datetime(*start_datetime) +
                    timedelta(days=1)).timetuple())[:6]
        except ValueError as err:
            print(f'Wrong datetime: {err}')
            return []

        return self.extract_time_period(
            records, start_datetime, end_datetime)

    def extract_last_date(self, raw_data: dict) -> List[bytes]:
        """Метод для извлечния из архива записей, соответсвующих
           последней дате.
        """
        return self.extract_one_date(
            raw_data['records'], raw_data['metadata']['max_datetime'])

    def __get_int_date(self, binary_data: bytes) -> int:
        """Метод для получения целого представления даты-времени
           из строки байтов.
        """
        return (struct.unpack('<H', binary_data)[0] >> 1)

    def split_records_by_dates(self, records: List[bytes]) -> Dict[str, list]:
        """Метод для разбиения извлеченных записей по отдельным датам.
        """
        if not records:
            return {}
        result = {}
        for record in records:
            int_date = self.__get_int_date(record[4:6])
            if int_date in result:
                result[int_date].append(record)
            else:
                result[int_date] = [record]
        return result

    def __get_bits_LE(self, i: int, bits_amount: int) -> List[int]:
        """Метод для получения битов числа в обратном порядке.
        """
        return [i >> j & 1 for j in range(bits_amount)]

    #! Если количество каналов 8, то ошибки и уставки помещаются в 1 байт.
    # Не понятно, как будет выглядеть все это для другого количества
    # каналов (4 и 16). По идее, надо передавать в метод второй параметр
    # frm = f'>{int((len(reading[9:]) - 1)/4)}fB' для чтения значений каналов,
    # но в таком случае код замедляется в полтора раза.
    # lim2 - подозреваю, что возможно, есть значение второй уставки.
    def decrypt_record(
        self, record: bytes
    ) -> Dict[int, Union[UnitDatetime, List[int], List[float], int]]:
        """Метод для декодирования записи.
        """
        _, length, int_dt, lim1, lim2, err = struct.unpack(
            '<2BI3B', record[:9])
        dt = self.__get_unit_datetime(int_dt)
        lim1 = self.__get_bits_LE(lim1, 8)
        # lim2 = self.__get_bits_LE(lim2, 8)
        err = self.__get_bits_LE(err, 8)
        *readings, cs = struct.unpack('>8fB', record[9:])
        readings = [el if not e else None for el, e in zip(readings, err)]

        return {
            'datetime': dt, 'readings': readings,
            'errors': err, 'limits': lim1, 'cs': cs}

    def decrypt_records(self, records: List[bytes]) -> List[dict]:
        """Метод для декодирования записей."""
        if not records:
            return []
        time_start = perf_counter()
        decrypted_records = [
            self.decrypt_record(record) for record in records
        ]
        print(
            f'{len(decrypted_records)} records were decrypted in',
            f'{(perf_counter() - time_start)*1e3:.2f} ms.'
        )
        time_start = perf_counter()
        result = sorted(decrypted_records, key=lambda d: d['datetime'])
        print(
            f'{len(result)} records were sorted in',
            f'{(perf_counter() - time_start)*1e3:.2f} ms.'
        )
        return result

    def convert_decrypted_record_to_str(self, record: dict, sep: str) -> str:
        """Метод для строкового представления записи из архива."""
        return sep.join(
            [
                '{2:02d}.{1:02d}.{0:d}{6}{3:02d}:{4:02d}:{5:02d}'.format(
                    *record['datetime'], sep
                ),
                *['{:.6f}'.format(r).replace('.', ',') if r is not None else
                 'None' for r in record['readings']
                ]
            ]
        )

    def float_or_None_to_str(self, value, tochn):
        if value is None:
            return 'None'
        s = f'{{:.{tochn}f}}'
        return s.format(value).replace('.', ',')

    def convert_decrypted_record_to_str_new(self, record: dict, sep: str, tochn: int) -> str:
        year, month, day, hour, minute, second = record['datetime']
        return sep.join(
            [f'{day:02d}.{month:02d}.{year:d}{sep}{hour:02d}:{minute:02d}:{second:02d}'] +
            [self.float_or_None_to_str(r, tochn) for r in record['readings']]
        )

    def convert_decrypted_record_to_str_new_2(self, record: dict, sep: str) -> str:
        year, month, day, hour, minute, second = record['datetime']
        return sep.join([
            f'{day:02d}.{month:02d}.{year:d}{sep}{hour:02d}:{minute:02d}:{second:02d}',
            *['{:.6f}'.format(r).replace('.', ',') if r is not None else
                 'None' for r in record['readings']
            ]]
        )    


    def write_decrypted_records_to_file(
        self,
        decrypted_records: List[dict],
        filename: str, sep: str
    ) -> None:
        """Метод для записи записей в текстовый файл."""
        try:
            with open(filename, 'w') as f:
                for decrypted_record in decrypted_records:
                    str_record = self.convert_decrypted_record_to_str(
                        decrypted_record, sep
                    )
                    f.write(f'{str_record}\n')
        except IOError:
            print(f'Error with <{filename}>.')

    def __create_filename(
        self, unit_number: int,
        start_datetime: UnitDatetime, end_datetime: UnitDatetime,
        frm=None, file_ext=None
    ) -> str:
        """Метод для создания имени файла."""

        dt_format = (frm or self.__datetime_format)
        _file_ext = (file_ext or self.__file_ext)
        sdt = dt_format.format(*start_datetime)
        edt = dt_format.format(*end_datetime)

        return '{}_{}-{}.{}'.format(unit_number, sdt, edt, _file_ext)

    def export_decrypted_records_to_file(
        self,
        records: List[dict],
        unit_number: int, sep: str
    ) -> None:
        """Метод для экспорта декодированных записей в текстовый файл.
        """
        filename = self.__create_filename(
            unit_number,
            records[0]['datetime'],
            records[-1]['datetime']
        )
        self.write_decrypted_records_to_file(records, filename, sep)

    def extract_last_date_from_outside(
        self, raw_data: dict, sep=None, write_to_file: bool = False
    ) -> List[dict]:
        """Метод для извлечения записей, соответствующих
           последней дате в архиве.
        """
        file_sep = (sep or self.__file_sep)
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
        start_datetime: UnitDatetime,
        end_datetime: UnitDatetime,
        sep=None,
        write_to_file: bool = False
    ) -> List[dict]:
        """Метод для извлечения заданного временного периода
           из архива.
        """
        file_sep = (sep or self.__file_sep)
        records = self.extract_time_period(
            raw_data['records'], start_datetime, end_datetime)
        decrypted_records = self.decrypt_records(records)
        if write_to_file:
            self.export_decrypted_records_to_file(
                decrypted_records,
                raw_data['metadata']['unit_number'],
                file_sep
            )
        return decrypted_records

    def write_csv(self, filename: str, data: List[dict]) -> None:
        """Метод для записи csv-файла
        """
        with open(filename, 'w') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerows(data)

    def convert_int_to_unit_date(
        self, int_date: int
    ) -> UnitDatetime:
        mask = [0b11111, 0b1111, 0b11111]
        shift = [0, 5, 9]
        dt = [int_date>>s & m for s, m in zip(shift, mask)]
        dt[-1] += 2000
        dt[-2] += 1
        dt[-3] += 1
        return tuple(dt[::-1])

def test_module_2() -> None:
    """Метод для тестирования модуля ``ar4_parser.py``.
    """
    filename = 'TM100514_B.AR4'
    ar4_parser = Ar4Parser()
    raw_data = ar4_parser.parse_ar4_file(filename)
    data = ar4_parser.extract_time_period_from_outside(
        raw_data, (2023, 8, 1, 0, 0 ,0), (2023, 10, 6, 0, 0, 0)
    )
    d = []
    for line in data:
        temp_list = [datetime(*line['datetime'])]
        temp_list += line['readings']
        d.append(temp_list)
    print(data[-1:])
    ar4_parser.write_csv('test.csv', d[-1:])
    time_start = perf_counter()
    res = []
    for r in data:
        res.append(ar4_parser.convert_decrypted_record_to_str(r, ';'))
    print(f'{perf_counter() - time_start} s.')
    print(res[-1], len(res))
    time_start = perf_counter()
    res = []
    for r in data:
        res.append(ar4_parser.convert_decrypted_record_to_str_new(r, ';', 6))
    print(f'{perf_counter() - time_start} s.')
    print(res[-1], len(res))
    time_start = perf_counter()
    res = []
    for r in data:
        res.append(ar4_parser.convert_decrypted_record_to_str_new_2(r, ';'))
    print(f'{perf_counter() - time_start} s.')
    print(res[-1], len(res))
    data = ar4_parser.extract_time_period(
        raw_data['records'], (2023, 8, 1, 0, 0 ,0), (2023, 10, 6, 0, 0, 0)
    )
    split_dates = ar4_parser.split_records_by_dates(data)
    print(
        *[
            ar4_parser.convert_int_to_unit_date(el) for
            el in list(split_dates.keys())
        ], sep='\n'
    )


def test_module() -> None:
    """Метод для тестирования модуля ``ar4_parser.py``.
    """
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
    print(
        data_1 == data_2,
        data_2 == data_3,
        data_3 == data_4,
        data_4 == data_1
    )
    print(len(data_1), len(data_2), len(data_3), len(data_4))

    data_5 = ar4_parser.extract_time_period_from_outside(
        raw_data, start_datetime, end_datetime, write_to_file=True)
    data_6 = ar4_parser.extract_last_date_from_outside(
        raw_data, write_to_file=True)

    print(data_5 == data_6, len(data_5), len(data_6))

    


if __name__ == '__main__':
    test_module_2()

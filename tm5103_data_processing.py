#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from argparse import ArgumentParser
from sources.ar4_parser import Ar4Parser
# from sources.tm5103_data_parser import TM5103DataParser
# from sources.tm5103_time_changer import TM5103TimeChanger
# from sources.tm5103_graph import TM5103GraphMaker


def read_settings(filename, _sep):
    result = {
        'output_dir': 'data_files',
        'channel_count': 4,
        'new_time': datetime.strptime('07:00:00', '%H:%M:%S')
    }
    settings = dict()
    try:
        with open(filename, 'r') as f:
            reader = csv.reader(f, delimiter=_sep)
            for row in reader:
                if len(row) == 2:
                    if row[0]:
                        settings[row[0]] = row[1]
    except IOError:
        print(f'I/O error. Please, check <{filename}>.')
    if settings.get('output_dir'):
        result['output_dir'] = settings['output_dir']
    if settings.get('channel_count'):
        try:
            result['channel_count'] = int(settings['channel_count'])
        except ValueError:
            print(f'Check {filename}: wrong <channel_count>')
    if settings.get('new_time'):
        try:
            result['new_time'] = datetime.strptime(settings['new_time'], '%H:%M:%S')
        except ValueError:
            print(f'Check {filename}: wrong <new_time>')
    return result  

# !TODO: добавить блоки try/except
def read_settings_new(filename: str, sep: str) -> dict:
    result = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line[-1] == '\n': line = line[:-1]
                key, value = line.split(sep)
                result[key] = value
    except IOError as err:
        print(err)
    result['chunk_size'] = int(result['chunk_size'])
    result['empty_byte'] = bytes.fromhex(result['empty_byte'])
    result['channels_amount'] = int(result['channels_amount'])
    return result

def create_parser():
    parser = ArgumentParser()
    # group = parser.add_mutually_exclusive_group()
    group = parser.add_argument_group()
    group.add_argument('-l', '--last-date', action='store_true')
    group.add_argument('-t', '--time-period', action='store_true')
    group.add_argument('-w', '--write-to-file', action='store_true')
    group.add_argument('-s', '--start-datetime', type=str)
    group.add_argument('-e', '--end-datetime', type=str)
    group.add_argument('-i', '--interactive', action='store_true')
    # group.add_argument('-s', '--split', action='store_true')
    # group.add_argument('-a', '--average', action='store_true')
    # group.add_argument('-t', '--time', action='store_true')
    # group.add_argument('-g', '--graph', action='store_true')
    # group.add_argument('-e', '--extract', action='store_true')
    # group.add_argument('-r', '--reduce', action='store_true')
    # group.add_argument('-c', '--columns', action='store_true')
    # parser.add_argument('filename', nargs='?')
    parser.add_argument('-f', '--filename', type=str, required=True)

    return parser

def my_cli(decrypted_records: list[dict]) -> None:
    print('Enter <r [int]> to reduce decrypted_records.' )
    print('Enter <e [start datetime] [end datetime]> to extract time period.')
    print('Enter <q> for exit.' )
    while True:
            input_str = input('-> ')
            if input_str == 'q': break
            my_args = input_str.split()
            if my_args[0] == 'r':
                if len(my_args) > 1:
                    try:
                        n = int(my_args[1])
                    except ValueError as err:
                        n = 1
                        print(err)
                    reduced_records = [r for i, r in enumerate(decrypted_records) if i % n == 0]
                    print(f'{len(decrypted_records)} records were reduced to {len(reduced_records)} records.')
                else:
                    print('Number should be placed after <r> flag.')
            if my_args[0] == 'e':
                if len(my_args) > 4:
                    sdt = datetime.strptime(' '.join([my_args[1], my_args[2]]), '%d.%m.%Y %H:%M:%S').timetuple()[:6]
                    edt = datetime.strptime(' '.join([my_args[3], my_args[4]]), '%d.%m.%Y %H:%M:%S').timetuple()[:6]
                    extracted_records = [r for r in decrypted_records if sdt <= r['datetime'] < edt]
                    print(f'{len(extracted_records)} records were extracted from {len(decrypted_records)} records.')
                else:
                    print('Not enough information of datetime.')

def main():
    print('This is tm5103 data processing!')

    config_file = 'settings.csv'
    settings = read_settings_new(config_file, '=')
    # print(settings)
    ar4_parser = Ar4Parser()
    ar4_parser.config_parser(settings)
    argparser = create_parser()
    # args = argparser.parse_args(['-f', './sources/TM100514_B.AR4', '-l'])
    args = argparser.parse_args(['-f', './sources/TM100514_B.AR4', '-t', '-s', '05.10.2023 00:00:00', '-e', '06.10.2023 00:00:00', '-wi'])
    # print(args)
    decrypted_records = []
    if args.last_date:
        raw_data = ar4_parser.parse_ar4_file(args.filename)
        decrypted_records = ar4_parser.extract_last_date_from_outside(
            raw_data,
            sep=settings['file_sep'],
            write_to_file=args.write_to_file)
    if args.time_period:
        # !TODO: add try/except blocks
        sdt = datetime.strptime(args.start_datetime, '%d.%m.%Y %H:%M:%S')
        start_datetime = sdt.timetuple()[:6]
        edt = datetime.strptime(args.end_datetime, '%d.%m.%Y %H:%M:%S')
        end_datetime = edt.timetuple()[:6]
        raw_data = ar4_parser.parse_ar4_file(args.filename)
        decrypted_records = ar4_parser.extract_time_period_from_outside(
            raw_data, start_datetime, end_datetime,
            sep=settings['file_sep'],
            write_to_file=args.write_to_file)
    if args.interactive:
        my_cli(decrypted_records)

if __name__ == '__main__':
    main()

    # # args = argparser.parse_args(['-s', './(2023_09_22)_RA.txt'])
    # # args = argparser.parse_args(['-g', './data/2023_09_22.txt'])
    # # args = argparser.parse_args(['-e', './(2023_09_22)_RA.txt'])
    # args = argparser.parse_args(['-c', './(2023_09_22).txt'])
    
    # if args.split:
    #     print('Split <%s>' % args.filename)
    #     output_dir = 'data_files'
    #     data_parser = TM5103DataParser()
    #     data_parser.parse_file(args.filename, output_dir)
    #     # data_parser.split_file(args.filename, 8)
    # elif args.extract:
    #     data_parser = TM5103DataParser()
    #     date = '29.09.2023'
    #     data = data_parser.extract_single_date(args.filename, date)
    #     if data:
    #         output_file = f'({"_".join(reversed(date.split(".")))}).txt' 
    #         data_parser.write_data_to_file(data, output_file)
    #     else:
    #         print(f'There is no such a date <{date}> in <{args.filename}>')
    # elif args.reduce:
    #     data_parser = TM5103DataParser()
    # elif args.columns:
    #     data_parser = TM5103DataParser()
    #     str_data = data_parser.extract_columns(args.filename, list(range(9)))
    #     data = data_parser.extract_data(str_data)
    #     reduced_data = data_parser.reduce_data(data, 27)
    #     # reduced_data = data_parser.reduce_data(data, 1)
    #     print(*reduced_data, sep='\n')
    # elif args.average:
    #     print('Average <%s>' % args.filename)
    # elif args.time:
    #     print('Time change at <%s>' % args.filename)
    #     time_changer = TM5103TimeChanger()
    #     time_changer.set_separator('\t')
    #     time_changer.set_time_format('%H:%M:%S')
    #     new_time = '07:00:00'
    #     time_changer.change_time(args.filename, new_time)
    # elif args.graph:
    #     graph_maker = TM5103GraphMaker()
    #     header = ['Время', 'ТП1', 'ТП2', 'ТП3', 'ТП4', 'ТП5', 'ТП6', 'ТП7', 'ТП8']
    #     graph_maker.create_graph('./sources/2023_09_22_processed.txt', header)
    #     print('Graph <%s>' % args.filename)

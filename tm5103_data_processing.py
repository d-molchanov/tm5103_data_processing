#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
from datetime import datetime
from argparse import ArgumentParser
from sources.tm5103_data_parser import TM5103DataParser
from sources.tm5103_time_changer import TM5103TimeChanger


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

def create_parser():
    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--split', action='store_true')
    group.add_argument('-a', '--average', action='store_true')
    group.add_argument('-t', '--time', action='store_true')
    group.add_argument('-g', '--graph', action='store_true')
    parser.add_argument('filename', nargs='?')

    return parser


if __name__ == '__main__':
    print('This is tm5103 data processing!')
    print(read_settings('settings.csv', ';'))
    argparser = create_parser()
    args = argparser.parse_args()
    if args.split:
        print('Split <%s>' % args.filename)
        output_dir = 'data_files'
        data_parser = TM5103DataParser()
        data_parser.parse_file(args.filename, output_dir)
        # data_parser.split_file(args.filename, 8)
    elif args.average:
        print('Average <%s>' % args.filename)
    elif args.time:
        print('Time change at <%s>' % args.filename)
        time_changer = TM5103TimeChanger()
        time_changer.set_separator('\t')
        time_changer.set_time_format('%H:%M:%S')
        new_time = '07:00:00'
        time_changer.change_time(args.filename, new_time)
    elif args.graph:
        print('Graph <%s>' % args.filename)

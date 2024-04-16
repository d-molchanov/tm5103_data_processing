#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime, timedelta
from typing import List, Union

import copy

from collections import abc
import json
import bisect
import argparse

import matplotlib as mpl
import matplotlib.pyplot as plt

class GraphMaker:

    def __init__(self):
        self.settings = {
            'title': '',
            'title settings': {
                'fontsize': 14,
                'loc': 'center',
                'pad': 40
            },
            'major grid': {
                'which': 'major',
                'c': '#aaaaaa50',
                'ls': '-'
            },
            'minor grid': {
                'which': 'minor',
                'c': '#aaaaaa50',
                'ls': ':'
            },
            'x major formatter': '%H:%M',
            'x major locator': {'byminute': [0, 60]},
            'x minor locator': {'byminute': [10, 20, 30, 40, 50]},
            # 'x minor locator': {'byminute': range(0, 60, 10)},
            'x label': 'Time',
            'x label settings': {'fontsize': 14},
            'y label': 'T, \u2103',
            'y label settings': {
                'fontsize': 14, 
                'rotation': 0
            },
            'y major locator': 100,
            'y minor locator': 20,
            'x lim': [None, None],
            'y lim': [0, 1100],
            'labels fontsize': 14,
            'tight layout': True,
            'fig size': [14, 10],
            'fig size settings': {'forward': True},
            'colors': [
                '#515151ff',
                '#f14040ff',
                '#1a6fdfff',
                '#37ad6bff',
                '#b177deff',
                '#cc9900ff',
                '#00ccbbff',
                '#7d4e4eff'
            ],
            'labels': [None, None, None, None, None, None, None, None],
            'plot': {'linewidth': 1},
            'legend': {
                'bbox_to_anchor': [0, 1, 1, 0], 
                'loc': 'lower left',
                'mode': 'expand',
                'ncol': 8,
                'fontsize': 14,
                'borderaxespad': 0.5
            },
            'tick_params': {
                'axis': 'x',
                'labelrotation': 0
            },
            'save to file': True,
            'output file': 'output.png',
            'savefig': {'bbox_inches': 'tight'}
        }

    def update_dict(self, old_dict: dict, new_dict: dict) -> None:
        if not isinstance(old_dict, abc.Mapping) or not isinstance(new_dict, abc.Mapping):
            return None
        for key, value in new_dict.items():
            if key in old_dict:
                if isinstance(old_dict[key], abc.Mapping):
                    if isinstance(value, abc.Mapping):
                        old_dict[key].update(value)
                else:
                    if not isinstance(value, abc.Mapping):
                        old_dict[key] = value
            else:
                old_dict[key] = value
        return None

    def config(self, settings: Union[str, dict]) -> None:
        if isinstance(settings, str):
            try:
                with open(settings) as f:
                    settings = json.load(f)
            except IOError as err:
                print(err)
        if isinstance(settings, abc.Mapping):
            self.update_dict(self.settings, settings)

    def export_config(self) -> None:
        output_file = self.settings['output file']
        if output_file == 'output.png':
            filename = 'graph_settings.json'
        else:
            filename =  f'{os.path.splitext(output_file)[0]}_graph_settings.json'
        try:
            with open(filename, 'w') as f:
                json.dump(self.settings, f, indent=4)
        except IOError as err:
            print(err)
        return None
    #! TODO: read extention for picture from config file



    def parse_float(self, str_value):
        try:
            return float(str_value)
        except ValueError:
            return None

    def parse_time(self, time):
        return datetime.strptime(time, '%H:%M:%S')

    def parse_file(self, filename):
        print(f'Start parsing <{filename}>')
        time_start = time.perf_counter()

        time_list = []
        value_matrix = []
        with open(filename, 'r') as f:
            first_line = f.readline().split(';')
            time_list.append(self.parse_time(first_line[1]))
            value_matrix = [[self.parse_float(el.replace(',', '.'))] for el in first_line[2:]]
            for line in f:
                split_line = line.split(';')
                time_list.append(self.parse_time(split_line[1]))
                for v, el in zip(value_matrix, split_line[2:]):
                    v.append(self.parse_float(el.replace(',', '.')))
        return {'time': time_list, 'values': value_matrix}

    def create_graph_new(self, datetimes: List[datetime], values: List[List[Union[float, None]]], settings: Union[dict, None]=None) -> None:
        if not settings:
            _settings = self.settings
        else:
            _settings = copy.deepcopy(self.settings)
            self.update_dict(_settings, settings)
        fig, ax = plt.subplots()
        plt.title(_settings['title'], **_settings['title settings'])
        ax.grid(**_settings['major grid'])
        ax.grid(**_settings['minor grid'])
        ax.set_xlabel(_settings['x label'], **_settings['x label settings'])
        ax.set_ylabel(_settings['y label'], **_settings['y label settings'])


        ax.xaxis.set_major_formatter(
            mpl.dates.DateFormatter(_settings['x major formatter']))
        ax.xaxis.set_major_locator(
            mpl.dates.MinuteLocator(**_settings['x major locator']))
        ax.xaxis.set_minor_locator(
            mpl.dates.MinuteLocator(**_settings['x minor locator']))

        ax.yaxis.set_major_locator(
            mpl.ticker.MultipleLocator(_settings['y major locator']))
        ax.yaxis.set_minor_locator(
            mpl.ticker.MultipleLocator(_settings['y minor locator']))

        ax.set_xlim(*_settings['x lim'])
        ax.set_ylim(*_settings['y lim'])

        for label in (ax.get_xticklabels() + ax.get_yticklabels()):
            label.set_fontsize(_settings['labels fontsize'])

        fig.set_size_inches(*_settings['fig size'], **_settings['fig size settings'])
        if _settings['tight layout']:
            fig.tight_layout()

        ax.tick_params(**_settings['tick_params'])

        for v, c, l in zip(values, _settings['colors'], _settings['labels']):
            ax.plot(datetimes, v, c, label=l, **_settings['plot'])

        ax.legend(**_settings['legend'])
        if _settings['save to file']:
            plt.savefig(_settings['output file'], **_settings['savefig'])
        else:
            plt.show()



    # def create_graph(self, header, time_list, value_matrix, graph_title, annotation, output_file):
    #     print('Start creating diagram')
    #     time_start = time.perf_counter()        
        

    #     colors = (
    #         '#515151ff',
    #         '#f14040ff',
    #         '#1a6fdfff',
    #         '#37ad6bff',
    #         '#b177deff',
    #         '#cc9900ff',
    #         '#00ccbbff',
    #         '#7d4e4eff'
    #     )

    #     fs = 14 
    #     labels = header
    #     fig, ax = plt.subplots()
    #     plt.title(graph_title, fontsize=fs, loc='center', pad=40)

    #     # ax.set_xlabel('Time', fontsize=fs)
    #     ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%H:%M"))
    #     maj_locator = mpl.dates.MinuteLocator(byminute=[0, 60])
    #     ax.xaxis.set_major_locator(maj_locator)
    #     ax.tick_params(axis='x', labelrotation = 0)
        
    #     min_locator = mpl.dates.MinuteLocator(byminute=range(0, 60, 10))
    #     ax.xaxis.set_minor_locator(min_locator)
    #     ax.set_xlim(time_list[0] - timedelta(minutes=5), time_list[-1] + timedelta(minutes=5))
    #     # ax.set_ylim(0, 1100)
    #     ax.set_ylim(1020, 1100)
    #     # ax.set_ylim(140, 1080)
    #     ax.yaxis.set_major_locator(mpl.ticker.MultipleLocator(100))
    #     ax.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(20))
    #     # ax.set_ylabel('T, \u2103', loc='top', fontsize=fs, rotation=0)
    #     ax.set_ylabel('T, \u2103', fontsize=fs, rotation=0)

    #     for v, l, c in zip(value_matrix, labels, colors):
    #         ax.plot(time_list, v, c, label=l, linewidth=1)
        
    #     # ax.legend(loc=0, fontsize=fs)
    #     ax.legend(bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=8, fontsize=fs, borderaxespad=0.5)
    #     # ax.legend(bbox_to_anchor=(1, 1), fontsize=fs)
    #     for label in (ax.get_xticklabels() + ax.get_yticklabels()):
    #         label.set_fontsize(fs)

    #     # otherwise the right y-label is slightly clipped
    #     fig.set_size_inches(14, 10, forward=True)
    #     fig.tight_layout()  
    #     # ax.grid(c='#aaaaaa50', ls='--')
    #     ax.grid(which='major', c='#aaaaaa50', ls='-')
    #     ax.grid(which='minor', c='#aaaaaa50', ls=':')
    #     total_time = time.perf_counter() - time_start
         
    #     connectionstyles = annotation['arrow_props'] 
       
    #     str_text_timestamps = annotation['text_coords']
    #     annotation_coordinates = [(time_list[self.find_nearest_index(el[0], time_list)], el[1]) for el in str_text_timestamps]
     
    #     str_arrow_timestamps = annotation['arrow_coords']
    #     arrow_coord_indices = [self.find_nearest_index(el, time_list) for el in str_arrow_timestamps]
    #     arrow_coordinates = [(time_list[i], v_m[i]) for i, v_m in zip(arrow_coord_indices, value_matrix)]
    #     self.create_annotation(plt, arrow_coordinates, header, annotation_coordinates, connectionstyles, fs)

    #     plt.savefig(output_file, bbox_inches='tight')

        # plt.show()

    # def find_nearest_index(self, str_time, time_list):
    #     timestamp = self.parse_time(str_time)
    #     index = bisect.bisect_right(time_list, timestamp)
    #     return index

    # def check_for_none(self, list_to_check):
    #     for el in list_to_check:
    #         if not el:
    #             return False
    #     return True

    # def create_annotation(self, plt, arr_coord, header, coords, connectionstyles, fs):
    #     bbox_prop = dict(boxstyle='square', edgecolor='k', facecolor='w', alpha=0.5)


    #     for a, h, c, s in zip(arr_coord, header, coords, connectionstyles):
    #         if self.check_for_none(a) and self.check_for_none(c):
    #             plt.annotate(
    #                 # h, xy=a
    #                 h, xy=a, xytext=c, 
    #                 bbox=bbox_prop, fontsize=fs,
    #                 arrowprops=dict(arrowstyle='->', connectionstyle=s)
    #             )
    #     return plt

    # def create_annotation_new(self, plt, arr_coord, header, coords, connectionstyles, fs):
    #     bbox_prop = dict(boxstyle='square', edgecolor='k', facecolor='w', alpha=0.5)


    #     for a, h, c, s in zip(arr_coord, header, coords, connectionstyles):
    #         if self.check_for_none(a) and self.check_for_none(c):
    #             plt.annotate(
    #                 # h, xy=a
    #                 h, xy=a, xytext=c, xycoords='figure_fraction',
    #                 bbox=bbox_prop, fontsize=fs,
    #                 arrowprops=dict(arrowstyle='->', connectionstyle=s)
    #             )
    #     return plt

    # def read_annotation(self, filename):
    #     result = {
    #         'text': [],
    #         'text_coords': [],
    #         'arrow_coords': [],
    #         'arrow_props': []
    #     }
    #     try:
    #         with open(filename, 'r') as f:
    #             for line in f:
    #                 split_line = line.split(';')
    #                 result['text'].append(split_line[0])
    #                 result['text_coords'].append((split_line[1], self.parse_float(split_line[2])))
    #                 result['arrow_coords'].append(split_line[3])
    #                 result['arrow_props'].append(split_line[4])
    #     except IOError:
    #         print(f'IOError with <{filename}>.')
    #     return result

def main():
    filename = '2023_10_05.csv'
    bname, ext = os.path.splitext(filename)
    #! TODO: read extention for picture from config file
    output_filename = f'{bname}.png'

    graph_maker = GraphMaker()
    # graph_maker.export_config()
    data = graph_maker.parse_file(filename)
    # graph_maker.export_config()
    sttngs = {
        'labels': [f'ТП{i}' for i in range(1, 9)],
        'save to file': False,
        # 'save to file': True,
        'x lim': [data['time'][0] - timedelta(minutes=10), data['time'][-1] + timedelta(minutes=10)],
        'output file': output_filename,
        'title': 'Hello!', 
        'title settings': {'fontsize': 72}
    }
    # graph_maker.config('graph_settings.json')
    # graph_maker.config(sttngs)
    graph_maker.create_graph_new(data['time'], data['values'], settings=sttngs) 
    # graph_maker.config({'x lim': [None, None]})
    # graph_maker.export_config()
    # graph_maker.create_graph_old(data['time'], data['values'], settings) 
    # graph_maker.create_graph_new(data['time'], data['values'], settings=settings) 


if __name__ == '__main__':

    main()


    # # 2024_04_13 =============================================================
    # # target_dir = os.path.abspath('D:/JIHT/!2024/!Ларина/!Processed_ED/(2024_03_13)')
    # target_dir = os.path.abspath('.')
    # prefix = os.path.split(target_dir)[1][1:-1]
    # suffixes = ['A', 'A_partial', 'B', 'B_partial']
    
    # ext = '.csv'
    # # filenames = [os.path.join(target_dir, f"{'_'.join([prefix, s])}{ext}") for s in suffixes]
    # filenames = ['2023_10_05.csv']
    # file_index = 0
    # print(*filenames, sep='\n')
    # filename = filenames[file_index]
    # title_left = '.'.join(reversed(prefix.split('_')))
    # reactor_number = {'A': 'А', 'B': 'Б'}
    # title_center = f'"Пироуглерод" {reactor_number[suffixes[file_index][0]]}'
    # # title_right = '"Шоковая" матрица, выдержка 2 часа'
    # title_right = ''
    # graph_title = '        '.join([title_left, title_center, title_right])
    # header = [f'ТП{i}' for i in range(1, 9)]
    # gp = GraphMaker()
    # path, fname = os.path.split(os.path.abspath(filename))
    # bname, ext = os.path.splitext(fname)
    # ann_filename = os.path.join(path, f'{bname}_annotation{ext}')
    # # output_file = os.path.join(path, f'{bname}.svg')
    # output_file = os.path.join(path, f'{bname}.png')
    
    # annotation = gp.read_annotation(ann_filename)
    # data = gp.parse_file(filename)
    # gp.create_graph(header, data['time'], data['values'], graph_title, annotation, output_file)



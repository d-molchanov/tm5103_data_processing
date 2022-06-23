#!/usr/bin/python
# -*- coding: utf-8 -*-
from argparse import ArgumentParser


def create_parser():
    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--split', action='store_true')
    group.add_argument('-a', '--average', action='store_true')
    group.add_argument('-g', '--graph', action='store_true')
    parser.add_argument('filename', nargs='?')

    return parser


if __name__ == '__main__':
    print('This is tm5103 data processing!')
    argparser = create_parser()
    args = argparser.parse_args()
    print(args)

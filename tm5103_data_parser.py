import os
import time


class TM5103DataParser():

    def create_output_dir(self, dir_name):
        if dir_name not in os.listdir():
            try:
                os.mkdir(dir_name)
            except OSError:
                print(f"Can't create <{dir_name}> directory")
            else:
                print(f"<{dir_name}> directory has been created")


    def parse_file(self, filename):
        print(f'Starting split of <{filename}> for data files.\n...')
        start_time = time.perf_counter()
        path = os.getcwd()
        output_dir = 'data_files'
        self.create_output_dir(output_dir)
        try:
            cur_date = ''
            with open(filename, 'r') as f:
                data = [
                    el for el in
                    f.readline().rstrip().split(' ') if el
                ]
                cur_date = data[0]
                print(data)
                output_file = ('%s.txt' %
                    '_'.join(reversed(cur_date.split('.')))
                )
                try:
                    w = open(f'{output_dir}/{output_file}', 'w')
                    str_data = '\t'.join(data)
                    w.write(f'{str_data}\n')
                except IOError:
                    print('Something has gone wrong!')
                for i, line in enumerate(f):
                    data = [
                        el for el in line.rstrip().split(' ') if el
                    ]
                    # print(i, data, sep=': ')
                    if data[0] == cur_date:
                        str_data = '\t'.join(data)
                        try:
                            w.write(f'{str_data}\n')
                        except IOError:
                            print('Something has gone wrong!')
                    else:
                        try:
                            w.close()
                            cur_date = data[0]
                            output_file = ('%s.txt' %
                               '_'.join(reversed(cur_date.split('.')))
                            )
                            w = open(f'{output_dir}/{output_file}', 'w')
                            str_data = '\t'.join(data)
                            w.write(f'{str_data}\n')
                        except IOError:
                            print('Something has gone wrong!')
                    # output_file = ('%s.txt' %
                    #     '_'.join(cur_date.split('.').reverse())
                    # )
                    # try:
                    #     with open(f'{output_file}.txt', 'w') as w:
                    #         pass
                    # except IOError:
                    #     pass
            parse_time = time.perf_counter() - start_time
            ms_time = round(parse_time * 1e3, 3)
            print(f'<{filename}> has been processed in {ms_time} ms.')
        except IOError:
            print(f'Something is wrong. Please, check {filename}.')

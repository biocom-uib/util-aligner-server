from contextlib import contextmanager
import csv
from io import StringIO

def iter_csv_fd(f, **kwargs):
    if 'skipinitialspace' not in kwargs and kwargs.get('delimiter',' ') == ' ':
        kwargs['skipinitialspace'] = True

    yield from csv.reader(f, **kwargs)

def iter_csv(file_path, **kwargs):
    with open(file_path, 'r') as f:
        yield from iter_csv_fd(f, **kwargs)

@contextmanager
def open_csv_write(file_path, **kwargs):
    with open(file_path, 'w+') as f:
        writer = csv.writer(f, **kwargs)
        yield writer

def write_csv(file_path, records, **kwargs):
    with open_csv_write(file_path, **kwargs) as writer:
        for record in records:
            writer.writerow(record)

def write_tsv_to_string(tuple_list, header=[]):
    f = StringIO()

    writer = csv.writer(f, delimiter='\t')

    if header:
        writer.writerow(header)
    writer.writerows(tuple_list)

    return f.getvalue()

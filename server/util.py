from contextlib import contextmanager
import csv
from io import StringIO
import pandas as pd


def all_equal(xs):
    it = iter(xs)

    try:
        x0 = next(it)
        while True:
            if x0 != next(it):
                return False
    except StopIteration:
        return True

def iter_csv_fd(f, header=False, **kwargs):
    if 'skipinitialspace' not in kwargs and kwargs.get('delimiter',' ') == ' ':
        kwargs['skipinitialspace'] = True

    reader = csv.reader(f, **kwargs)

    if header:
        next(reader)

    yield from reader


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

def write_tsv_to_string(table, header=[]):
    if isinstance(table, pd.DataFrame):
        return table.to_csv(sep='\t', index=False, header = header or True)

    elif isinstance(table, pd.Series):
        return table.to_csv(sep='\t', index=False, header = header or True)

    else:
        f = StringIO()
        writer = csv.writer(f, delimiter='\t')

        if header:
            writer.writerow(header)
        writer.writerows(table)

        return f.getvalue()

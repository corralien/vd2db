import re
import pathlib
import mmap
from collections import defaultdict
import pandas as pd


def read_vdfile(vdfile: pathlib.Path) -> (str, pd.DataFrame):
    with (open(vdfile, mode='rb') as fp,
        mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as mm):

        params = {}
        while row := mm.readline().strip().decode():
            if sre := re.search(r'^\*\s*(?P<key>[^-]+)-\s*(?P<val>.+)', row):
                params[sre.group('key')] = sre.group('val')

        scenario = params['ImportID'].split(':')[-1]
        dtypes = defaultdict(lambda: str, **{params['ValueDim']: float})

        options = {'comment': '*', 'header': None, 'dtype': dtypes, 'na_values': ['-', 'NONE', 'none'],
                   'sep': params['FieldSeparator'], 'quotechar': params['TextDelim'],
                   'encoding_errors': 'replace', 'low_memory': False}

        veda = pd.read_csv(mm, names=params['Dimensions'].split(';'), **options)
        return scenario, veda

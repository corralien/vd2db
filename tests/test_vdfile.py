import pathlib

import pandas as pd

from vd2db.vdfile import read_vdfile


def test_read_vdfile():
    data_dir = pathlib.Path(__file__).parent / "data"
    scenario, veda = read_vdfile(data_dir / "baseline.vd")
    assert scenario == 'baseline'
    assert isinstance(veda, pd.DataFrame)
    assert not veda.empty

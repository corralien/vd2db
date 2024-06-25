import pathlib
from pytest import fixture
from vd2db.vdfile import read_vdfile
import pandas as pd

def test_read_vdfile():
    data_dir = pathlib.Path(__file__).parent / "data"
    scenario, veda = read_vdfile(data_dir / "baseline.vd")
    assert scenario == 'baseline'
    assert isinstance(veda, pd.DataFrame)
    assert not veda.empty

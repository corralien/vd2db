# test_vdbase.py

import sqlite3
import pandas as pd
import pytest
import pathlib
from jinja2 import Template
from vd2db.vdbase import VDBase, DIMENSIONS
from vd2db.vdfile import read_vdfile


#@pytest.fixture(scope='class')
#def db(tmp_path_factory):
#    db = tmp_path_factory.mktemp("vd2db") / "test.db"
#    yield db
#    db.unlink()

@pytest.fixture
def vdbase():
    return VDBase(":memory:")

@pytest.fixture(scope="class")
def vdfiles():
    data_dir = pathlib.Path(__file__).parent / "data"
    vdfiles = [read_vdfile(vdfile) for vdfile in data_dir.glob('*.vd')]
    return vdfiles

class TestVDBase:

    def test_init(self, vdbase):
        assert isinstance(vdbase.cursor, sqlite3.Cursor)
        stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        for table in DIMENSIONS:
            result = vdbase.connection.execute(stmt, (table,)).fetchone()
            assert result is not None, f"Table {table} does not exist"

    def test_repr(self, vdbase):
        assert repr(vdbase) == f"VDBase(db=':memory:')"

    def test_connection(self, vdbase):
        assert isinstance(vdbase.connection, sqlite3.Connection)

    def test_cursor(self, vdbase):
        assert isinstance(vdbase.cursor, sqlite3.Cursor)

    def test_scenarios(self, vdbase):
        scenarios = vdbase.scenarios
        assert isinstance(scenarios, pd.DataFrame)
        assert list(scenarios.columns) == ["ID", "Name", "created_at", "updated_at"]

    def test_import_from(self, vdbase, vdfiles):
        for scenario, veda in vdfiles:
            vdbase.import_from(scenario, veda.copy())
            assert vdbase.scenarios['Name'].eq(scenario).any()
        assert len(vdbase.scenarios) == len(vdfiles)

    def test_import_existing_scenario(self, vdbase, vdfiles):
        scenario, veda = vdfiles[0]
        vdbase.import_from(scenario, veda.copy())
        with pytest.raises(sqlite3.IntegrityError, match=f"Scenario '{scenario}' already exists."):
            vdbase.import_from(scenario, veda.copy())

    def test_remove(self, vdbase, vdfiles):
        scenario, veda = vdfiles[0]
        vdbase.import_from(scenario, veda.copy())
        vdbase.remove(scenario)
        assert vdbase.scenarios['Name'].ne(scenario).all()

    def test_compact(self, vdbase, vdfiles):
        scenario, veda = vdfiles[0]
        vdbase.import_from(scenario, veda.copy())
        vdbase.remove(scenario)

        ps1 = vdbase.cursor.execute('PRAGMA page_size').fetchone()[0]
        pc1 = vdbase.cursor.execute('PRAGMA page_count').fetchone()[0]
        vdbase.compact()
        ps2 = vdbase.cursor.execute('PRAGMA page_size').fetchone()[0]
        pc2 = vdbase.cursor.execute('PRAGMA page_count').fetchone()[0]
        assert (ps1 * pc1) > (ps2 * pc2)

    def test_close(self, vdbase):
        vdbase.close()
        with pytest.raises(sqlite3.ProgrammingError, match="Cannot operate on a closed database."):
            scenarios = vdbase.scenarios


import pathlib
import sqlite3
from jinja2 import Template
import pandas as pd
from vd2db.vdfile import read_vdfile


create_dim_table = """\
CREATE TABLE IF NOT EXISTS {{ dimension }} (
    ID INTEGER NOT NULL,
    {% if dimension == 'Scenario' -%}
    Name VARCHAR(255) NOT NULL,
    created_at DATETIME,
    updated_at DATETIME,
    {%- else %}
    Name VARCHAR(64),
    {%- endif %}
    UNIQUE(Name),
    PRIMARY KEY (ID)
)"""

create_attr_table = """\
CREATE TABLE IF NOT EXISTS _{{ attribute }} (
    ID INTEGER NOT NULL,
    {%- for dim in dimensions %}
    {{ dim }} INTEGER,
    {%- endfor %}
    PV FLOAT,
    {%- for dim in dimensions %}
    FOREIGN KEY({{ dim }}) REFERENCES {{ dim }} (ID)
        ON DELETE CASCADE ON UPDATE NO ACTION,
    {%- endfor %}
    PRIMARY KEY (ID)
)"""

create_attr_view = """\
CREATE VIEW IF NOT EXISTS {{ attribute }} AS
    SELECT
    {%- for dim in dimensions %}
        {{ dim }}.Name AS {{ dim }},
    {%- endfor %}
        PV
    FROM _{{ attribute }}
    {%- for dim in dimensions %}
    LEFT JOIN {{ dim }} ON _{{ attribute }}.{{ dim }} = {{ dim }}.ID
    {%- endfor -%}
"""

DIMENSIONS = ['Scenario', 'Attribute', 'Sow', 'Commodity', 'Process',
              'Period', 'Region', 'Vintage', 'TimeSlice', 'UserConstraint']


class VDBase:

    def __init__(self, db):
        self._vdb = db
        self._conn = sqlite3.connect(db)
        self._curs = self._conn.cursor()

        # Init database
        tmpl = Template(create_dim_table)
        for dim in DIMENSIONS:
            self.cursor.execute(tmpl.render(dimension=dim))
            if dim != 'Scenario':
                stmt = f"INSERT OR IGNORE INTO {dim} (ID, Name) VALUES (0, NULL)"
                self.cursor.execute(stmt)
        self.commit()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._curs

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def __repr__(self):
        return f"VDBase(db='{self._vdb}')"

    @property
    def scenarios(self):
        """List all database scenarios."""
        res = self.cursor.execute('SELECT * FROM Scenario')
        df = pd.DataFrame(res, columns=['ID', 'Name', 'created_at', 'updated_at'])
        return df

    def import_from(self, scenario, veda):
        """Import veda scenario to database."""
        # 1. Prepare data
        veda.insert(0, 'Scenario', pd.Series(scenario, veda.index, dtype=str))
        if self.scenarios['Name'].eq(scenario).any():
            raise sqlite3.IntegrityError(f"Scenario '{scenario}' already exists.")

        dims = veda.columns.intersection(DIMENSIONS)
        tables = veda.groupby('Attribute')
        dimensions = {attr: df.columns[~df.isna().all()]
                      for attr, df in tables}

        # 2. Create (missing) attribute tables
        res = self.cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '#_%' ESCAPE '#'")
        attributes = pd.DataFrame(res, columns=['Table']).squeeze()
        table_tmpl = Template(create_attr_table)
        view_tmpl = Template(create_attr_view)
        for attr, dims in dimensions.items():
            if attr not in attributes:
                self.cursor.execute(table_tmpl.render(attribute=attr, dimensions=dims[:-1]))
                self.cursor.execute(view_tmpl.render(attribute=attr, dimensions=dims[:-1]))
        self.connection.commit()

        # 3. Import (missing) attribute labels
        name2id = {}
        for dim in veda.columns.intersection(DIMENSIONS):
            # Load existing labels
            res = self.cursor.execute(f"SELECT Name FROM {dim}")
            sr1 = pd.DataFrame(res, columns=[dim])[dim]
            sr2 = veda[dim].drop_duplicates().dropna()

            # Insert new labels
            stmt = f"INSERT INTO {dim} (Name) VALUES (?)"
            diff = sr2[~sr2.isin(sr1)]
            self.cursor.executemany(stmt, diff.to_frame().to_records(index=False).tolist())

            # Reload labels to get ID
            res = self.cursor.execute(f"SELECT ID, Name FROM {dim}")
            dmap = pd.DataFrame(res, columns=['ID', 'Name']).set_index('Name')['ID']
            name2id[dim] = dmap
        self.connection.commit()

        # 4. Label to ID conversion
        cols = veda.columns.intersection(DIMENSIONS)
        veda[cols] = veda[cols].apply(lambda x: x.map(name2id[x.name]))

        # 5. Insert records from scenario
        CHUNKSIZE = 100000
        for attr, df in tables:
            dims = dimensions[attr]
            stmt = f"INSERT INTO _{attr} ({', '.join(dims)}) VALUES ({', '.join('?' * len(dims))})"
            for idx in range(0, len(df), CHUNKSIZE):
                chunk = df.iloc[idx:idx+CHUNKSIZE, [df.columns.get_loc(dim) for dim in dims]]
                self.cursor.executemany(stmt, chunk.to_records(index=False).tolist())
        self.connection.commit()

    def remove(self, scenario):
        """Remove one scenario."""
        if self.scenarios['Name'].eq(scenario).any():
            self.cursor.execute("PRAGMA foreign_keys=1")
            stmt = f"DELETE FROM Scenario WHERE Name = (?)"
            self.cursor.execute(stmt, (scenario,))
            self.connection.commit()
            self.cursor.execute("PRAGMA foreign_keys=0")


    def compact(self):
        """Shrink database."""
        self.cursor.execute("VACUUM")


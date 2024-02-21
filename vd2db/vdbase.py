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
        self.db = db
        self.con = sqlite3.connect(self.db)

        # Init database
        tmpl = Template(create_dim_table)
        for dim in DIMENSIONS:
            self.con.execute(tmpl.render(dimension=dim))
            if dim != 'Scenario':
                stmt = f"INSERT OR IGNORE INTO {dim} (ID, Name) VALUES (0, NULL)"
                self.con.execute(stmt)
        self.con.commit()

    def __repr__(self):
        return f"VDBase(db='{self.db}')"


    def connect(self, with_fk=False):
        """Connect to database."""
        con = sqlite3.connect(self.db)
        stmt = f"PRAGMA foreign_keys={int(with_fk)}"
        con.execute(stmt)
        return con


    @property
    def scenarios(self):
        """List all database scenarios."""
        con = self.connect()
        cur = self.con.execute('SELECT * FROM Scenario')
        con.close()
        return pd.DataFrame(cur, columns=['ID', 'Name', 'created_at', 'updated_at'])


    def import_from(self, vdfile):
        """Import vdfile to database."""
        con = self.connect()
        # 1. Load data from VD file
        scen, veda = read_vdfile(vdfile)
        if self.scenarios['Name'].eq(scen).any():
            raise sqlite3.IntegrityError(f"Scenario '{scen}' already exists.")

        dims = veda.columns.intersection(DIMENSIONS)
        tables = veda.groupby('Attribute')
        dimensions = {attr: df.columns[~df.isna().all()]
                      for attr, df in tables}

        # 2. Create (missing) attribute tables
        cur = con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '#_%' ESCAPE '#'")
        attributes = pd.DataFrame(cur, columns=['Table']).squeeze()
        table_tmpl = Template(create_attr_table)
        view_tmpl = Template(create_attr_view)
        for attr, dims in dimensions.items():
            if attr not in attributes:
                con.execute(table_tmpl.render(attribute=attr, dimensions=dims[:-1]))
                con.execute(view_tmpl.render(attribute=attr, dimensions=dims[:-1]))
        con.commit()

        # 3. Import (missing) attribute labels
        name2id = {}
        for dim in veda.columns.intersection(DIMENSIONS):
            # Load existing labels
            cur = con.execute(f"SELECT Name FROM {dim}")
            sr1 = pd.DataFrame(cur, columns=[dim])[dim]
            sr2 = veda[dim].drop_duplicates().dropna()

            # Insert new labels
            stmt = f"INSERT INTO {dim} (Name) VALUES (?)"
            diff = sr2[~sr2.isin(sr1)]
            con.executemany(stmt, diff.to_frame().to_records(index=False).tolist())

            # Reload labels to get ID
            cur = con.execute(f"SELECT ID, Name FROM {dim}")
            dmap = pd.DataFrame(cur, columns=['ID', 'Name']).set_index('Name')['ID']
            name2id[dim] = pd.concat([pd.Series({float('nan'): 0}), dmap])
        con.commit()

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
                con.executemany(stmt, chunk.to_records(index=False).tolist())
        con.commit()
        con.close()


    def remove(self, scenario):
        """Remove one scenario."""
        if self.scenarios['Name'].eq(scenario).any():
            con = self.connect(with_fk=True)
            stmt = f"DELETE FROM Scenario WHERE Name = (?)"
            con.execute(stmt, (scenario,))
            con.commit()
            con.close()


    def compact(self):
        """Shrink database."""
        con = self.connect()
        stmt = "VACUUM"
        con.execute(stmt)
        con.close()


import click
import pandas as pd
import pathlib
from vd2db.vdbase import VDBase
from vd2db.vdfile import read_vdfile

APP_NAME = 'vd2db'

CONFIG_DIR = pathlib.Path(click.get_app_dir(APP_NAME))
DATA_DIR = pathlib.Path.home() / APP_NAME


@click.group()
@click.version_option()
def cli():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


cli.epilog = f"Run 'vd2db COMMAND --help' for more information on a command."


# Database commands
@cli.group(name='database')
def db_cli():
    """Manage VEDA databases."""
    pass


@db_cli.command(name='init')
@click.argument('dbname')
def db_init(dbname):
    """Initialize a new database."""
    db = VDBase(DATA_DIR / f'{dbname}.db')


@db_cli.command(name='list')
def db_list():
    """List existing databases."""
    for db in sorted(DATA_DIR.glob('*.db')):
        click.echo(f'- {db.stem}')


@db_cli.command(name='delete')
@click.argument('dbname')
def db_delete(dbname):
    """Delete database."""
    (DATA_DIR / f'{dbname}.db').unlink()


# Scenario commands
@cli.group(name='scenario')
def sc_cli():
    """Manage scenarios in a database."""
    pass


@sc_cli.command(name='list')
@click.argument('dbname', nargs=1, required=True)
def sc_list(dbname):
    """List scenarios in specified database."""
    db = VDBase(DATA_DIR / f'{dbname}.db')
    scenarios = db.scenarios
    click.echo(f'{len(scenarios)} scenario(s) found in "{dbname}" database:')
    for scen in scenarios['Name']:
        click.echo(f'- {scen}')


@sc_cli.command(name='import')
@click.option('--as',  'newname',  nargs=1, default=None, help='Rename imported scenario')
@click.argument('vdfile', type=click.Path(path_type=pathlib.Path), nargs=1, required=True)
@click.argument('dbname', nargs=1, required=True)
def sc_import(vdfile, dbname, newname):
    """Import specified scenario."""
    db = VDBase(DATA_DIR / f'{dbname}.db')
    scenario, veda = read_vdfile(vdfile)
    scenario = newname or scenario
    db.import_from(scenario, veda)


@sc_cli.command(name='remove')
@click.argument('scenario', nargs=1, required=True)
@click.argument('dbname', nargs=1, required=True)
def remove_scenario(scenario, dbname):
    """Remove specified  scenario."""
    db = VDBase(DATA_DIR / f'{dbname}.db')
    db.remove(scenario)


if __name__ == '__main__':
    cli()

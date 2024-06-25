# vd2db

**vd2db** is a command-line tool that allows you to import VD files (TIMES Scenarios) into a SQLite database. Once imported, the data can be used via ODBC by Excel, PowerQuery, PowerBI, and other data analysis tools.

## Installation

Ensure you have Python 3.x installed on your machine (tested on 3.9, 3.10, and 3.11). Then, you can install `vd2db` via one of the following methods:

### Method 1: Install directly from GitHub

```sh
pip install git+https://github.com/corralien/vd2db.git
```

### Method 2: Clone the repository and install locally

```sh
git clone https://github.com/corralien/vd2db.git
cd vd2db
pip install .
```

## Usage

The basic usage of `vd2db` is through the command line. Here are the different commands and options available. You can use the --help parameter at any time to get contextual help.

## Main use cases

#### Start a new study

Initialize a new database (the database will be created in the directory `$HOME/vd2db` on Linux/MacOS or `%HomeDrive%%HomePath%/vd2db` on Windows):

```sh
[...]$ vd2db database init mystudy
```

#### Import scenarios

Import three scenarios (`baseline.vd`, `netzero.vd`, `tax.vd`) into the database:

```sh
[...]$ vd2db scenario import baseline.vd mystudy
[...]$ vd2db scenario import netzero.vd mystudy
[...]$ vd2db scenario import tax.vd mystudy
```

#### List scenarios

List the scenarios contained in the database:

```sh
[...]$ vd2db scenario list mystudy
3 scenario(s) found in "mystudy" database:
- baseline
- netzero
- tax
```

#### Remove a scenario

Remove a scenario (`tax`) from the database:

```sh
[...]$ vd2db scenario remove tax mystudy
```


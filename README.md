# fasql (v0.2.0)

> Read FASTX Files w/ DuckDB

- [Overview](#overview)
  - [Schemas](#schemas)
    - [FASTA](#fasta)
    - [FASTQ](#fastq)
  - [Replacement Scans](#replacement-scans)
  - [Globs](#globs)
- [Installation and Usage](#installation-and-usage)
  - [DuckDB Console](#duckdb-console)
  - [Python](#python)

## Overview

`fasql` is an open source DuckDB Extension from [WHERE TRUE Technologies](https://www.wheretrue.com) that adds FASTA and FASTQ file parsing as table functions.

For example, given a FASTA file called `./swissprot.fasta.gz` in your local directory, you can query it like so.

```sql
SELECT *
FROM read_fasta('./swissprot.fasta.gz')
LIMIT 5
-- ┌──────────────────────┬──────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬──────────────────────┐
-- │          id          │     description      │                                                           sequence                                                           │      file_name       │
-- │       varchar        │       varchar        │                                                           varchar                                                            │       varchar        │
-- ├──────────────────────┼──────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼──────────────────────┤
-- │ sp|A0A023I7E1|ENG1…  │ Glucan endo-1,3-be…  │ MRFQVIVAAATITMITSYIPGVASQSTSDGDDLFVPVSNFDPKSIFPEIKHPFEPMYANTENGKIVPTNSWISNLFYPSADNLAPTTPDPYTLRLLDGYGGNPGLTIRQPSAKVLGSYPPTN…  │ ./swissprot.fasta.gz │
-- │ sp|A0A024B7W1|POLG…  │ Genome polyprotein…  │ MKNPKKKSGGFRIVNMLKRGVARVSPFGGLKRLPAGLLLGHGPIRMVLAILAFLRFTAIKPSLGLINRWGSVGKKEAMEIIKKFKKDLAAMLRIINARKEKKRRGADTSVGIVGLLLTTAMA…  │ ./swissprot.fasta.gz │
-- │ sp|A0A024SC78|CUTI…  │ Cutinase OS=Hypocr…  │ MRSLAILTTLLAGHAFAYPKPAPQSVNRRDWPSINEFLSELAKVMPIGDTITAACDLISDGEDAAASLFGISETENDPCGDVTVLFARGTCDPGNVGVLVGPWFFDSLQTALGSRTLGVKGV…  │ ./swissprot.fasta.gz │
-- │ sp|A0A024SH76|GUX2…  │ Exoglucanase 2 OS=…  │ MIVGILTTLATLATLAASVPLEERQACSSVWGQCGGQNWSGPTCCASGSTCVYSNDYYSQCLPGAASSSSSTRAASTTSRVSPTTSRSSSATPPPGSTTTRVPPVGSGTATYSGNPFVGVTP…  │ ./swissprot.fasta.gz │
-- │ sp|A0A026W182|ORCO…  │ Odorant receptor c…  │ MMKMKQQGLVADLLPNIRVMKTFGHFVFNYYNDNSSKYLHKVYCCVNLFMLLLQFGLCAVNLIVESADVDDLTANTITLLFFTHSIVKICYFAIRSKYFYRTWAIWNNPNSHPLFAESNARY…  │ ./swissprot.fasta.gz │
-- └──────────────────────┴──────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────┘
```

### Schemas

The schemas for the FASTA and FASTQ table functions are as follows.

#### FASTA

| column_name | column_type | null |
| ----------- | ----------- | ---- |
| id          | VARCHAR     | YES  |
| description | VARCHAR     | YES  |
| sequence    | VARCHAR     | YES  |
| file_name   | VARCHAR     | YES  |

#### FASTQ

| column_name    | column_type | null |
| -------------- | ----------- | ---- |
| id             | VARCHAR     | YES  |
| description    | VARCHAR     | YES  |
| sequence       | VARCHAR     | YES  |
| quality_scores | VARCHAR     | YES  |
| file_name      | VARCHAR     | YES  |

### Replacement Scans

A number of "replacement scans" also work, whereby you just need to have a file reasonably named, and the extension will pick up on it as the appropriate file. E.g. `SELECT * FROM 'test.fasta'` or `SELECT * FROM 'test.fastq.gz'`.

The following file endings will trigger the scan for FASTA:

* `.fasta.gz`
* `.fa.gz`
* `.fasta`
* `.fa`

And for FASTQ:

* `.fastq.gz`
* `.fq.gz`
* `.fastq`
* `.fq`

### Globs

Globs are supported both within the table function and the replacement scan (provided the glob matches the replacement scan in the first place.

For example, `SELECT * FROM './path/to/*.fasta'` will select all FASTA files in the `./path/to/` directory. This is the same as `SELECT * FROM read_fasta('./path/to/*.fasta')`.

## Installation and Usage

You can use this extension as you would other DuckDB extensions. Here's one example of how to do that in a raw DuckDB console and one in Python.

### DuckDB Console

To install and use `fasql`, start a `duckdb` session:

```
# unsigned required for non duckdb distributed extensions
$ duckdb -unsigned
```

Now from the session:

```SQL
D SET custom_extension_repository='fasql.wheretrue.com/fasql/latest';
D INSTALL fasql;
D LOAD fasql;
```

If you want some easy data to play with you can `wget` one of the test files.

```console
$ wget https://raw.githubusercontent.com/wheretrue/fasql/main/test/sql/test.fasta
```

Then back into the console, load the extension and enjoy.

```SQL
D LOAD fasql;
D SELECT COUNT(*) FROM read_fasta('./test.fasta');
-- ┌──────────────┐
-- │ count_star() │
-- │    int64     │
-- ├──────────────┤
-- │            2 │
-- └──────────────┘
```

### Python

For example, this script installs the extension then counts the number of records at `path`.

```python
import pathlib

import duckdb

con = duckdb.connect(config={'allow_unsigned_extensions': True})
con.execute("SET custom_extension_repository='fasql.wheretrue.com/fasql/latest';")
con.execute("INSTALL fasql;")
con.execute("LOAD fasql;")

# Assumes this is in your home directory.
path = pathlib.Path("swissprot.fasta.gz")

result = con.execute(f"SELECT * FROM read_fasta('{path}');").fetchall()
print(len(result))
assert len(result) == 569213

# Or create a dataframe.
df = con.execute(f"SELECT id, description, sequence FROM read_fasta('{path}');").df()
# print(df.head())
#                           id                                        description                                           sequence
# 0   sp|A0A023I7E1|ENG1_RHIMI  Glucan endo-1,3-beta-D-glucosidase 1 OS=Rhizom...  MRFQVIVAAATITMITSYIPGVASQSTSDGDDLFVPVSNFDPKSIF...
# 1   sp|A0A024B7W1|POLG_ZIKVF  Genome polyprotein OS=Zika virus (isolate ZIKV...  MKNPKKKSGGFRIVNMLKRGVARVSPFGGLKRLPAGLLLGHGPIRM...
# 2  sp|A0A024SC78|CUTI1_HYPJR  Cutinase OS=Hypocrea jecorina (strain ATCC 567...  MRSLAILTTLLAGHAFAYPKPAPQSVNRRDWPSINEFLSELAKVMP...
# 3   sp|A0A024SH76|GUX2_HYPJR  Exoglucanase 2 OS=Hypocrea jecorina (strain AT...  MIVGILTTLATLATLAASVPLEERQACSSVWGQCGGQNWSGPTCCA...
# 4   sp|A0A026W182|ORCO_OOCBI  Odorant receptor coreceptor OS=Ooceraea biroi ...  MMKMKQQGLVADLLPNIRVMKTFGHFVFNYYNDNSSKYLHKVYCCV...
```

# fasql (0.0.2)

> Read FASTX Files w/ DuckDB

## Overview

`fasql` is an open source DuckDB Extension from WHERE TRUE Technologies that adds FASTA and FASTQ file parsing as table functions.

For example, given a FASTA file called `./swissprot.fasta.gz` in your local directory, you can query it like so.

```sql
SELECT *
FROM read_fasta('./swissprot.fasta.gz')
LIMIT 5
-- ┌──────────────────────┬──────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
-- │          id          │     description      │                                                                                             sequence                                                                                              │
-- │       varchar        │       varchar        │                                                                                              varchar                                                                                              │
-- ├──────────────────────┼──────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ sp|A0A023I7E1|ENG1…  │ Glucan endo-1,3-be…  │ MRFQVIVAAATITMITSYIPGVASQSTSDGDDLFVPVSNFDPKSIFPEIKHPFEPMYANTENGKIVPTNSWISNLFYPSADNLAPTTPDPYTLRLLDGYGGNPGLTIRQPSAKVLGSYPPTNDVPYTDAGYMINSVVVDLRLTSSEWSDVVPDRQVTDWDHLSANLRLSTPQDSNSYIDFPIVRGMAYITA…  │
-- │ sp|A0A024B7W1|POLG…  │ Genome polyprotein…  │ MKNPKKKSGGFRIVNMLKRGVARVSPFGGLKRLPAGLLLGHGPIRMVLAILAFLRFTAIKPSLGLINRWGSVGKKEAMEIIKKFKKDLAAMLRIINARKEKKRRGADTSVGIVGLLLTTAMAAEVTRRGSAYYMYLDRNDAGEAISFPTTLGMNKCYIQIMDLGHMCDATMSYECPMLDEGVEPDDVDCWC…  │
-- │ sp|A0A024SC78|CUTI…  │ Cutinase OS=Hypocr…  │ MRSLAILTTLLAGHAFAYPKPAPQSVNRRDWPSINEFLSELAKVMPIGDTITAACDLISDGEDAAASLFGISETENDPCGDVTVLFARGTCDPGNVGVLVGPWFFDSLQTALGSRTLGVKGVPYPASVQDFLSGSVQNGINMANQIKSVLQSCPNTKLVLGGYSQGSMVVHNAASNLDAATMSKISAVVLF…  │
-- │ sp|A0A024SH76|GUX2…  │ Exoglucanase 2 OS=…  │ MIVGILTTLATLATLAASVPLEERQACSSVWGQCGGQNWSGPTCCASGSTCVYSNDYYSQCLPGAASSSSSTRAASTTSRVSPTTSRSSSATPPPGSTTTRVPPVGSGTATYSGNPFVGVTPWANAYYASEVSSLAIPSLTGAMATAAAAVAKVPSFMWLDTLDKTPLMEQTLADIRTANKNGGNYAGQFV…
-- │ sp|A0A026W182|ORCO…  │ Odorant receptor c…  │ MMKMKQQGLVADLLPNIRVMKTFGHFVFNYYNDNSSKYLHKVYCCVNLFMLLLQFGLCAVNLIVESADVDDLTANTITLLFFTHSIVKICYFAIRSKYFYRTWAIWNNPNSHPLFAESNARYHAIALKKMRLLLFLVGGTTMLAAVAWTVLTFFEHPIRKIVDPVTNETEIIELPQLLIRSFYPFDAGKGI…  │
-- └──────────────────────┴──────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Installation

To install and use `fasql` you, start a `duckdb` session:

```
# -unsigned required for non duckdb distributed extensions
$ duckdb -unsigned
```

Now from the session:

```SQL
D SET custom_extension_repository='fasql.wheretrue.com/fasql/latest'
D INSTALL fasql
D LOAD fasql
```

If you want some easy data to play with you can `wget` one of the test files.

```console
$ wget https://raw.githubusercontent.com/wheretrue/fasql/main/test/sql/test.fasta
```

Then back into the console, load the extension and enjoy.

```SQL
D LOAD fasql
D SELECT COUNT(*) FROM read_fasta('./test.fasta')
-- ┌──────────────┐
-- │ count_star() │
-- │    int64     │
-- ├──────────────┤
-- │            2 │
-- └──────────────┘
```

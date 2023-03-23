# fasql (0.0.2)

> Read FASTX Files w/ DuckDB

## Overview

`fasql` is a DuckDB Extension that adds FASTA and FASTQ file parsing as table functions to DuckDB.

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

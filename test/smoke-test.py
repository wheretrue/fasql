import duckdb
import pathlib

con = duckdb.connect(config={'allow_unsigned_extensions': True})

con.execute("SET custom_extension_repository='fasql.wheretrue.com/fasql/latest';")
con.execute("INSTALL fasql;")
con.execute("LOAD fasql;")

path = pathlib.Path(__file__).parent / "sql" / "test.fasta"

result = con.execute(f"SELECT * FROM read_fasta('{path}');").fetchall()
print(len(result))
assert len(result) == 2

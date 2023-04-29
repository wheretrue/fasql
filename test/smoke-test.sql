SET custom_extension_repository='dbe.wheretrue.com/fasql/latest';
INSTALL fasql;
LOAD fasql;

SELECT COUNT(*) FROM read_fasta('./test/sql/test.fasta');

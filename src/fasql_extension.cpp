#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "fasql_extension.hpp"
#include "fasta_io.hpp"
#include "fastq_io.hpp"

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {
        Connection con(instance);
        con.BeginTransaction();

        auto &context = *con.context;
        auto &catalog = Catalog::GetSystemCatalog(context);

        auto fasta_scan = fasql::FastaIO::GetFastaTableFunction();
        catalog.CreateTableFunction(context, fasta_scan.get());

        auto fastq_scan = fasql::FastqIO::GetFastqTableFunction();
        catalog.CreateTableFunction(context, fastq_scan.get());

        auto &config = DBConfig::GetConfig(context);

        auto fasta_replacement_scan = fasql::FastaIO::GetFastaReplacementScanFunction;
        config.replacement_scans.emplace_back(fasta_replacement_scan);

        auto fastq_replacement_scan = fasql::FastqIO::GetFastqReplacementScanFunction;
        config.replacement_scans.emplace_back(fastq_replacement_scan);

        auto fasta_copy = fasql::FastaIO::GetFastaCopyFunction();
        catalog.CreateCopyFunction(context, fasta_copy.get());

        con.Commit();
    }

    void FasqlExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }

    std::string FasqlExtension::Name()
    {
        return "fasql";
    }

}

extern "C"
{

    DUCKDB_EXTENSION_API void fasql_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *fasql_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

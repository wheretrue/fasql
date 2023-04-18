#include <duckdb.hpp>

#include <kseq++/seqio.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace fasql
{

    class FastaIO
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetFastaTableFunction();
        static duckdb::unique_ptr<duckdb::TableRef> GetFastaReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data);

#if defined(__APPLE__) || defined(__linux__)
        static duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> GetFastaCopyFunction();
#endif
    };

}

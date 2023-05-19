#include <duckdb.hpp>

#include <kseq++/seqio.hpp>

#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

using namespace duckdb;
namespace fasql
{

    class FastaIO
    {
    public:
        static unique_ptr<CreateTableFunctionInfo> GetFastaTableFunction();
        static unique_ptr<TableRef> GetFastaReplacementScanFunction(ClientContext &context, const std::string &table_name, ReplacementScanData *data);

#if defined(__APPLE__) || defined(__linux__)
        static CreateCopyFunctionInfo GetFastaCopyFunction();
#endif
    };

}

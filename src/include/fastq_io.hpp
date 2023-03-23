#include <duckdb.hpp>

#include <kseq++/seqio.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace fasql
{

    class FastqIO
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetFastqTableFunction();
    };

}

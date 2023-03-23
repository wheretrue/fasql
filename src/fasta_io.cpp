#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <string>
#include <vector>

#include "fasta_io.hpp"

#include <kseq++/seqio.hpp>

namespace fasql
{

    struct FastaScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        klibpp::SeqStreamIn *stream;
    };

    struct FastaScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
    };

    struct FastaScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        FastaScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> FastaInitGlobalState(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<FastaScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastaInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastaScanBindData *)input.bind_data;
        auto &gstate = (FastaScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastaScanLocalState>();

        return move(local_state);
    }

    duckdb::unique_ptr<duckdb::FunctionData> FastaBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                       std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<FastaScanBindData>();
        auto file_name = input.inputs[0].GetValue<std::string>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!fs.FileExists(file_name))
        {
            throw duckdb::IOException("File does not exist: " + file_name);
        }

        result->file_path = file_name;
        result->stream = new klibpp::SeqStreamIn(result->file_path.c_str());

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");

        return move(result);
    }

    void FastaScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const FastaScanBindData *)data.bind_data;
        auto local_state = (FastaScanLocalState *)data.local_state;

        if (local_state->done)
        {
            return;
        }

        auto stream = bind_data->stream;
        auto records = stream->read(STANDARD_VECTOR_SIZE);

        auto read_records = 0;

        for (auto &record : records)
        {
            output.SetValue(0, output.size(), duckdb::Value(record.name));

            if (record.comment.empty())
            {
                output.SetValue(1, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(1, output.size(), duckdb::Value(record.comment));
            }

            output.SetValue(2, output.size(), duckdb::Value(record.seq));

            output.SetCardinality(output.size() + 1);

            read_records++;
        }

        if (read_records < STANDARD_VECTOR_SIZE)
        {
            local_state->done = true;
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> FastaIO::GetFastaTableFunction()
    {
        auto fasta_table_function = duckdb::TableFunction("read_fasta", {duckdb::LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);

        duckdb::CreateTableFunctionInfo fasta_table_function_info(fasta_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(fasta_table_function_info);
    }

}

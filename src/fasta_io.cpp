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
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastaInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastaScanBindData *)input.bind_data;
        auto &gstate = (FastaScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastaScanLocalState>();

        return std::move(local_state);
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

        return std::move(result);
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

    duckdb::unique_ptr<duckdb::TableRef> FastaIO::GetFastaReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_fasta_filename = duckdb::StringUtil::EndsWith(table_name, ".fa") || duckdb::StringUtil::EndsWith(table_name, ".fasta");
        valid_fasta_filename = valid_fasta_filename || duckdb::StringUtil::EndsWith(table_name, ".fa.gz") || duckdb::StringUtil::EndsWith(table_name, ".fasta.gz");

        if (!valid_fasta_filename)
        {
            return nullptr;
        };

        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!(fs.FileExists(table_name)))
        {
            return nullptr;
        };

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fasta", std::move(children));

        return table_function;
    }

}

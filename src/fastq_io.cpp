#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <string>
#include <vector>

#include "fastq_io.hpp"

#include <kseq++/seqio.hpp>

namespace fasql
{

    struct FastqScanBindData : public duckdb::TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

        klibpp::SeqStreamIn *stream;
    };

    struct FastqScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
    };

    struct FastqScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        FastqScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> FastqInitGlobalState(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<FastqScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastqInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastqScanBindData *)input.bind_data;
        auto &gstate = (FastqScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastqScanLocalState>();

        return std::move(local_state);
    }

    duckdb::unique_ptr<duckdb::FunctionData> FastqBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                       std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<FastqScanBindData>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        auto glob = input.inputs[0].GetValue<std::string>();
        std::vector<std::string> glob_result = fs.Glob(glob);
        if (glob_result.size() == 0)
        {
            throw duckdb::IOException("No files found for glob: " + glob);
        }
        result->file_paths = glob_result;
        result->stream = new klibpp::SeqStreamIn(result->file_paths[0].c_str());

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");
        names.push_back("quality_scores");
        names.push_back("file_name");

        return std::move(result);
    }

    void FastqScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (FastqScanBindData *)data.bind_data;
        auto local_state = (FastqScanLocalState *)data.local_state;

        if (local_state->done)
        {
            return;
        }

        auto stream = bind_data->stream;
        auto nth_file = bind_data->nth_file;
        auto current_file = bind_data->file_paths[nth_file];
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

            if (record.qual.empty())
            {
                output.SetValue(3, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(3, output.size(), duckdb::Value(record.qual));
            }

            output.SetValue(4, output.size(), duckdb::Value(current_file));

            output.SetCardinality(output.size() + 1);

            read_records++;
        }

        if (read_records < STANDARD_VECTOR_SIZE)
        {
            if (bind_data->nth_file < bind_data->file_paths.size() - 1)
            {
                bind_data->nth_file++;
                bind_data->stream = new klibpp::SeqStreamIn(bind_data->file_paths[bind_data->nth_file].c_str());
            }
            else
            {
                local_state->done = true;
            }
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> FastqIO::GetFastqTableFunction()
    {
        auto fastq_table_function = duckdb::TableFunction("read_fastq", {duckdb::LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);

        duckdb::CreateTableFunctionInfo fastq_table_function_info(fastq_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(fastq_table_function_info);
    }

    duckdb::unique_ptr<duckdb::TableRef> FastqIO::GetFastqReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_fasta_filename = duckdb::StringUtil::EndsWith(table_name, ".fq") || duckdb::StringUtil::EndsWith(table_name, ".fastq");
        valid_fasta_filename = valid_fasta_filename || duckdb::StringUtil::EndsWith(table_name, ".fq.gz") || duckdb::StringUtil::EndsWith(table_name, ".fastq.gz");

        if (!valid_fasta_filename)
        {
            return nullptr;
        };

        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        std::vector<std::string> glob_result = fs.Glob(table_name);
        if (glob_result.size() == 0)
        {
            return nullptr;
        }

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fastq", std::move(children));

        return table_function;
    }

}

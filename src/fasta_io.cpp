#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <iostream>
#include <string>
#include <vector>

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#endif

#include "fasta_io.hpp"

#include <kseq++/seqio.hpp>
#include <kseq++/kseq++.hpp>

using namespace klibpp;

namespace fasql
{

    struct FastaScanBindData : public duckdb::TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

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

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");
        names.push_back("file_name");

        return std::move(result);
    }

    void FastaScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (FastaScanBindData *)data.bind_data;
        auto local_state = (FastaScanLocalState *)data.local_state;

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
            output.SetValue(3, output.size(), duckdb::Value(current_file));

            output.SetCardinality(output.size() + 1);

            read_records++;
        }

        // We have read all records from the current file, check if we have more files or are done.
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

        std::vector<std::string> glob_result = fs.Glob(table_name);
        if (glob_result.size() == 0)
        {
            return nullptr;
        }

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fasta", std::move(children));

        return table_function;
    }

#if defined(__APPLE__) || defined(__linux__)
    struct FastaCopyScanOptions
    {
    };

    struct FastaWriteBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
    };

    struct FastaWriteGlobalState : public duckdb::GlobalFunctionData
    {
        int file_descriptor;
        klibpp::KStream<int, ssize_t (*)(int __fd, const void *__buf, size_t __nbyte), klibpp::mode::Out_> stream;

        // an example of an alternative constructor
        FastaWriteGlobalState(int fd)
            : file_descriptor(fd), stream(make_kstream(fd, write, klibpp::mode::out))
        {
        }
    };

    struct FastaCopyBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        klibpp::SeqStreamIn *in_stream;
    };

    duckdb::unique_ptr<duckdb::FunctionData>
    FastaCopyToBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastaWriteBindData>();
        result->file_name = info.file_path;

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto copy_to_file_exists = fs.FileExists(result->file_name);

        if (copy_to_file_exists)
        {
            throw std::runtime_error("File already exists, please remove.");
        }

        if (!(names == std::vector<std::string>{"id", "description", "sequence"} || names == std::vector<std::string>{"id", "sequence"}))
        {
            throw std::runtime_error("Invalid column names for FASTA file, expected 'id', 'description' and 'sequence' or 'id' and 'sequence'.");
        }

        // check everything is varchars
        for (auto &type : sql_types)
        {
            if (type.id() != duckdb::LogicalTypeId::VARCHAR)
            {
                throw std::runtime_error("Invalid column type for FASTA file, expected VARCHAR.");
            }
        }

        return std::move(result);
    };

    static duckdb::unique_ptr<duckdb::GlobalFunctionData> FastaWriteInitializeGlobal(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, const std::string &file_path)
    {
        auto &fasta_write_bind = (FastaWriteBindData &)bind_data;
        auto file_name = fasta_write_bind.file_name;

        auto compression = duckdb::StringUtil::EndsWith(file_name, ".gz");

        auto file_pointer = open(file_name.c_str(), O_WRONLY | O_CREAT, 0644);

        auto global_state = duckdb::make_unique<FastaWriteGlobalState>(file_pointer);

        return std::move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> FastaWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return std::move(local_data);
    }

    static void FastaWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                               duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        auto &bind_data = (FastaWriteBindData &)bind_data_p;
        auto &global_state = (FastaWriteGlobalState &)gstate;

        // First throw an error if the input size isn't two or three, then get a boolean for which is the case.
        if (input.size() != 2 && input.size() != 3)
        {
            throw std::runtime_error("FastaWriteSink: input.size() != 2 && input.size() != 3");
        }

        auto two_columns = input.data.size() == 2;

        if (two_columns)
        {
            auto &id = input.data[0];
            auto &sequence = input.data[1];

            for (size_t i = 0; i < input.size(); i++)
            {
                auto id_str = id.GetValue(i).ToString();
                auto sequence_str = sequence.GetValue(i).ToString();

                // Create the kseq record
                // auto record = klibpp::KSeq(id_str, "", sequence_str, "");
                auto record = klibpp::KSeq{
                    id_str,
                    "",
                    sequence_str,
                    "",
                };

                global_state.stream << record;
            }
        }
        else
        {
            auto &id = input.data[0];
            auto &description = input.data[1];
            auto &sequence = input.data[2];

            for (size_t i = 0; i < input.size(); i++)
            {
                auto id_str = id.GetValue(i).ToString();
                auto description_str = description.GetValue(i).ToString();
                auto sequence_str = sequence.GetValue(i).ToString();

                auto record = klibpp::KSeq{
                    id_str,
                    description_str,
                    sequence_str,
                    "",
                };

                global_state.stream << record;
            }
        }
    };

    static void FastaWriteCombine(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate, duckdb::LocalFunctionData &lstate)
    {
    }

    void FastaWriteFinalize(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate)
    {
        auto &global_state = (FastaWriteGlobalState &)gstate;

        global_state.stream << klibpp::kend;
        close(global_state.file_descriptor);
    };

    static duckdb::unique_ptr<duckdb::FunctionData> FastaCopyBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastaCopyBindData>();

        // Check that the input names are correct
        if (names.size() == 3)
        {
            if (names[0] != "id" || names[1] != "description" || names[2] != "sequence")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, description, sequence)");
            }
        }
        else if (names.size() == 2)
        {
            if (names[0] != "id" || names[1] != "sequence")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, sequence)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, description, sequence) or (id, sequence)");
        }

        // Check the input types are correct, if 2 or 3 length is allowed and all must be varchars
        if (sql_types.size() == 3)
        {
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR || sql_types[2] != duckdb::LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else if (sql_types.size() == 2)
        {
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR) or (VARCHAR, VARCHAR)");
        }

        result->file_name = info.file_path;
        result->in_stream = new klibpp::SeqStreamIn(result->file_name.c_str());

        return std::move(result);
    }

    duckdb::CopyFunction CreateFastaCopyFunction()
    {

        duckdb::CopyFunction function("fasta");

        function.copy_to_bind = FastaCopyToBind;
        function.copy_to_initialize_global = FastaWriteInitializeGlobal;
        function.copy_to_initialize_local = FastaWriteInitializeLocal;

        function.copy_to_sink = FastaWriteSink;
        function.copy_to_combine = FastaWriteCombine;
        function.copy_to_finalize = FastaWriteFinalize;

        function.copy_from_bind = FastaCopyBind;

        auto fasta_scan_function = duckdb::TableFunction("read_fasta", {duckdb::LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);

        function.copy_from_function = fasta_scan_function;

        function.extension = "fasta";
        return function;
    }

    duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> FastaIO::GetFastaCopyFunction()
    {
        auto function = CreateFastaCopyFunction();
        duckdb::CreateCopyFunctionInfo info(function);

        return duckdb::make_unique<duckdb::CreateCopyFunctionInfo>(info);
    };
#endif
}

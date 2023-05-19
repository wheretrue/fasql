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
using namespace duckdb;

namespace fasql
{

    struct FastaScanBindData : public TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

        klibpp::SeqStreamIn *stream;
    };

    struct FastaScanLocalState : public LocalTableFunctionState
    {
        bool done = false;
    };

    struct FastaScanGlobalState : public GlobalTableFunctionState
    {
        FastaScanGlobalState() : GlobalTableFunctionState() {}
    };

    unique_ptr<GlobalTableFunctionState> FastaInitGlobalState(ClientContext &context,
                                                              TableFunctionInitInput &input)
    {
        auto result = make_uniq<FastaScanGlobalState>();
        return std::move(result);
    }

    unique_ptr<LocalTableFunctionState> FastaInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state)
    {
        // auto bind_data = (const FastaScanBindData *)input.bind_data;
        // auto &gstate = (FastaScanGlobalState &)*global_state;

        auto local_state = make_uniq<FastaScanLocalState>();

        return std::move(local_state);
    }

    unique_ptr<FunctionData> FastaBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names)
    {
        auto result = make_uniq<FastaScanBindData>();
        auto &fs = FileSystem::GetFileSystem(context);

        auto glob = input.inputs[0].GetValue<std::string>();
        std::vector<std::string> glob_result = fs.Glob(glob);
        if (glob_result.size() == 0)
        {
            throw IOException("No files found for glob: " + glob);
        }

        result->file_paths = glob_result;
        result->stream = new klibpp::SeqStreamIn(result->file_paths[0].c_str());

        return_types.push_back(LogicalType::VARCHAR);
        return_types.push_back(LogicalType::VARCHAR);
        return_types.push_back(LogicalType::VARCHAR);
        return_types.push_back(LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");
        names.push_back("file_name");

        return std::move(result);
    }

    void FastaScan(ClientContext &context, TableFunctionInput &data, DataChunk &output)
    {
        auto &bind_data = (FastaScanBindData &)*data.bind_data;
        auto &local_state = (FastaScanLocalState &)*data.local_state;

        if (local_state.done)
        {
            return;
        }

        auto stream = bind_data.stream;
        auto nth_file = bind_data.nth_file;
        auto current_file = bind_data.file_paths[nth_file];
        auto records = stream->read(STANDARD_VECTOR_SIZE);

        auto read_records = 0;

        for (auto &record : records)
        {
            output.SetValue(0, output.size(), Value(record.name));

            if (record.comment.empty())
            {
                output.SetValue(1, output.size(), Value());
            }
            else
            {
                output.SetValue(1, output.size(), Value(record.comment));
            }

            output.SetValue(2, output.size(), Value(record.seq));
            output.SetValue(3, output.size(), Value(current_file));

            output.SetCardinality(output.size() + 1);

            read_records++;
        }

        // We have read all records from the current file, check if we have more files or are done.
        if (read_records < STANDARD_VECTOR_SIZE)
        {
            if (bind_data.nth_file < bind_data.file_paths.size() - 1)
            {
                bind_data.nth_file++;
                bind_data.stream = new klibpp::SeqStreamIn(bind_data.file_paths[bind_data.nth_file].c_str());
            }
            else
            {
                local_state.done = true;
            }
        }
    };

    unique_ptr<CreateTableFunctionInfo> FastaIO::GetFastaTableFunction()
    {
        auto fasta_table_function = TableFunction("read_fasta", {LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);

        CreateTableFunctionInfo fasta_table_function_info(fasta_table_function);
        return make_uniq<CreateTableFunctionInfo>(fasta_table_function_info);
    }

    unique_ptr<TableRef> FastaIO::GetFastaReplacementScanFunction(ClientContext &context, const std::string &table_name, ReplacementScanData *data)
    {
        auto table_function = make_uniq<TableFunctionRef>();

        auto valid_fasta_filename = StringUtil::EndsWith(table_name, ".fa") || StringUtil::EndsWith(table_name, ".fasta");
        valid_fasta_filename = valid_fasta_filename || StringUtil::EndsWith(table_name, ".fa.gz") || StringUtil::EndsWith(table_name, ".fasta.gz");

        if (!valid_fasta_filename)
        {
            return nullptr;
        };

        auto &fs = FileSystem::GetFileSystem(context);

        std::vector<std::string> glob_result = fs.Glob(table_name);
        if (glob_result.size() == 0)
        {
            return nullptr;
        }

        std::vector<unique_ptr<ParsedExpression>> children;
        children.push_back(make_uniq<ConstantExpression>(Value(table_name)));

        table_function->function = make_uniq<FunctionExpression>("read_fasta", std::move(children));

        return table_function;
    }

#if defined(__APPLE__) || defined(__linux__)
    struct FastaCopyScanOptions
    {
    };

    struct FastaWriteBindData : public TableFunctionData
    {
        std::string file_name;
    };

    struct FastaWriteGlobalState : public GlobalFunctionData
    {
        int file_descriptor;
        klibpp::KStream<int, ssize_t (*)(int __fd, const void *__buf, size_t __nbyte), klibpp::mode::Out_> stream;

        // an example of an alternative constructor
        FastaWriteGlobalState(int fd)
            : file_descriptor(fd), stream(make_kstream(fd, write, klibpp::mode::out))
        {
        }
    };

    struct FastaCopyBindData : public TableFunctionData
    {
        std::string file_name;
        klibpp::SeqStreamIn *in_stream;
    };

    unique_ptr<FunctionData>
    FastaCopyToBind(ClientContext &context, CopyInfo &info, vector<std::string> &names, vector<LogicalType> &sql_types)
    {
        auto result = make_uniq<FastaWriteBindData>();
        result->file_name = info.file_path;

        auto &fs = FileSystem::GetFileSystem(context);
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
            if (type.id() != LogicalTypeId::VARCHAR)
            {
                throw std::runtime_error("Invalid column type for FASTA file, expected VARCHAR.");
            }
        }

        return std::move(result);
    };

    static unique_ptr<GlobalFunctionData> FastaWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data, const std::string &file_path)
    {
        auto &fasta_write_bind = (FastaWriteBindData &)bind_data;
        auto file_name = fasta_write_bind.file_name;

        auto compression = StringUtil::EndsWith(file_name, ".gz");

        auto file_pointer = open(file_name.c_str(), O_WRONLY | O_CREAT, 0644);

        auto global_state = make_uniq<FastaWriteGlobalState>(file_pointer);

        return std::move(global_state);
    }

    static unique_ptr<LocalFunctionData> FastaWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data)
    {
        auto local_data = make_uniq<LocalFunctionData>();
        return std::move(local_data);
    }

    static void FastaWriteSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input)
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

    static void FastaWriteCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &lstate)
    {
    }

    void FastaWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate)
    {
        auto &global_state = (FastaWriteGlobalState &)gstate;

        global_state.stream << klibpp::kend;
        close(global_state.file_descriptor);
    };

    static unique_ptr<FunctionData> FastaCopyBind(ClientContext &context, CopyInfo &info, vector<std::string> &names, vector<LogicalType> &sql_types)
    {
        auto result = make_uniq<FastaCopyBindData>();

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
            if (sql_types[0] != LogicalType::VARCHAR || sql_types[1] != LogicalType::VARCHAR || sql_types[2] != LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else if (sql_types.size() == 2)
        {
            if (sql_types[0] != LogicalType::VARCHAR || sql_types[1] != LogicalType::VARCHAR)
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

    CopyFunction CreateFastaCopyFunction()
    {

        CopyFunction function("fasta");

        function.copy_to_bind = FastaCopyToBind;
        function.copy_to_initialize_global = FastaWriteInitializeGlobal;
        function.copy_to_initialize_local = FastaWriteInitializeLocal;

        function.copy_to_sink = FastaWriteSink;
        function.copy_to_combine = FastaWriteCombine;
        function.copy_to_finalize = FastaWriteFinalize;

        function.copy_from_bind = FastaCopyBind;

        auto fasta_scan_function = TableFunction({LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);

        function.copy_from_function = fasta_scan_function;

        function.extension = "fasta";
        return function;
    }

    CreateCopyFunctionInfo FastaIO::GetFastaCopyFunction()
    {
        auto function = CreateFastaCopyFunction();
        CreateCopyFunctionInfo info(function);

        return CreateCopyFunctionInfo(info);
    };
#endif
}

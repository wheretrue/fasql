#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <string>

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#endif

#include "fastq_io.hpp"

#include <kseq++/seqio.hpp>
#include <kseq++/kseq++.hpp>

using namespace duckdb;
namespace fasql
{

    struct FastqScanBindData : public TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

        klibpp::SeqStreamIn *stream;
    };

    struct FastqScanLocalState : public LocalTableFunctionState
    {
        bool done = false;
    };

    struct FastqScanGlobalState : public GlobalTableFunctionState
    {
        FastqScanGlobalState() : GlobalTableFunctionState() {}
    };

    unique_ptr<GlobalTableFunctionState> FastqInitGlobalState(ClientContext &context,
                                                              TableFunctionInitInput &input)
    {
        auto result = make_uniq<FastqScanGlobalState>();
        return std::move(result);
    }

    unique_ptr<LocalTableFunctionState> FastqInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state)
    {
        auto local_state = make_uniq<FastqScanLocalState>();

        return std::move(local_state);
    }

    unique_ptr<FunctionData> FastqBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names)
    {
        auto result = make_uniq<FastqScanBindData>();
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
        return_types.push_back(LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");
        names.push_back("quality_scores");
        names.push_back("file_name");

        return std::move(result);
    }

    void FastqScan(ClientContext &context, TableFunctionInput &data, DataChunk &output)
    {
        auto bind_data = data.bind_data->Cast<FastqScanBindData>();
        auto local_state = data.local_state->Cast<FastqScanLocalState>();

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

            if (record.qual.empty())
            {
                output.SetValue(3, output.size(), Value());
            }
            else
            {
                output.SetValue(3, output.size(), Value(record.qual));
            }

            output.SetValue(4, output.size(), Value(current_file));

            output.SetCardinality(output.size() + 1);

            read_records++;
        }

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

    unique_ptr<CreateTableFunctionInfo> FastqIO::GetFastqTableFunction()
    {
        auto scan = TableFunction("read_fastq", {LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);

        CreateTableFunctionInfo fastq_table_function_info(scan);
        return make_uniq<CreateTableFunctionInfo>(fastq_table_function_info);
    }

    unique_ptr<TableRef> FastqIO::GetFastqReplacementScanFunction(ClientContext &context, const std::string &table_name, ReplacementScanData *data)
    {
        auto table_function = make_uniq<TableFunctionRef>();

        auto valid_fasta_filename = StringUtil::EndsWith(table_name, ".fq") || StringUtil::EndsWith(table_name, ".fastq");
        valid_fasta_filename = valid_fasta_filename || StringUtil::EndsWith(table_name, ".fq.gz") || StringUtil::EndsWith(table_name, ".fastq.gz");

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

        table_function->function = make_uniq<FunctionExpression>("read_fastq", std::move(children));

        return table_function;
    }
#if defined(__APPLE__) || defined(__linux__)
    struct FastqCopyScanOptions
    {
    };

    struct FastqWriteBindData : public TableFunctionData
    {
        std::string file_name;
    };

    struct FastqWriteGlobalState : public GlobalFunctionData
    {
        int file_descriptor;
        klibpp::KStream<int, ssize_t (*)(int __fd, const void *__buf, size_t __nbyte), klibpp::mode::Out_> stream;

        // an example of an alternative constructor
        FastqWriteGlobalState(int fd)
            : file_descriptor(fd), stream(klibpp::make_kstream(fd, write, klibpp::mode::out))
        {
        }
    };

    struct FastqCopyBindData : public TableFunctionData
    {
        std::string file_name;
        klibpp::SeqStreamIn *in_stream;
    };

    unique_ptr<FunctionData>
    FastqCopyToBind(ClientContext &context, CopyInfo &info, vector<std::string> &names, vector<LogicalType> &sql_types)
    {
        auto result = make_uniq<FastqWriteBindData>();
        result->file_name = info.file_path;

        auto &fs = FileSystem::GetFileSystem(context);
        auto copy_to_file_exists = fs.FileExists(result->file_name);

        if (copy_to_file_exists)
        {
            throw std::runtime_error("File already exists, please remove.");
        }

        if (!(names == std::vector<std::string>{"id", "description", "sequence", "quality_scores"} || names == std::vector<std::string>{"id", "sequence", "quality_scores"}))
        {
            throw std::runtime_error("Invalid column names for FASTQ file, expected id, description, sequence, quality_scores or id, sequence, quality_scores.");
        }

        for (auto &type : sql_types)
        {
            if (type.id() != LogicalTypeId::VARCHAR)
            {
                throw std::runtime_error("Invalid column type for FASTA file, expected VARCHAR.");
            }
        }

        return std::move(result);
    };

    static unique_ptr<GlobalFunctionData> FastqWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data, const std::string &file_path)
    {
        auto &fasta_write_bind = (FastqWriteBindData &)bind_data;
        auto file_name = fasta_write_bind.file_name;

        auto compression = StringUtil::EndsWith(file_name, ".gz");

        auto file_pointer = open(file_name.c_str(), O_WRONLY | O_CREAT, 0644);

        auto global_state = make_uniq<FastqWriteGlobalState>(file_pointer);

        return std::move(global_state);
    }

    static unique_ptr<LocalFunctionData> FastqWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data)
    {
        auto local_data = make_uniq<LocalFunctionData>();
        return std::move(local_data);
    }

    static void FastqWriteSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input)
    {
        auto &bind_data = (FastqWriteBindData &)bind_data_p;
        auto &global_state = (FastqWriteGlobalState &)gstate;

        // First throw an error if the input size isn't two or three, then get a boolean for which is the case.
        if (input.size() != 2 && input.size() != 3)
        {
            throw std::runtime_error("FastqWriteSink: input.size() != 3 && input.size() != 4");
        }

        auto three_columns = input.data.size() == 3;

        if (three_columns)
        {
            auto &id = input.data[0];
            auto &sequence = input.data[1];
            auto &quality_scores = input.data[2];

            for (size_t i = 0; i < input.size(); i++)
            {
                auto id_str = id.GetValue(i).ToString();
                auto sequence_str = sequence.GetValue(i).ToString();
                auto quality_scores_str = quality_scores.GetValue(i).ToString();

                // Create the kseq record
                auto record = klibpp::KSeq{
                    id_str,
                    "",
                    sequence_str,
                    quality_scores_str,
                };

                global_state.stream << record;
            }
        }
        else
        {
            auto &id = input.data[0];
            auto &description = input.data[1];
            auto &sequence = input.data[2];
            auto &quality_scores = input.data[3];

            for (size_t i = 0; i < input.size(); i++)
            {
                auto id_str = id.GetValue(i).ToString();
                auto description_str = description.GetValue(i).ToString();
                auto sequence_str = sequence.GetValue(i).ToString();
                auto quality_scores_str = quality_scores.GetValue(i).ToString();

                auto record = klibpp::KSeq{
                    id_str,
                    description_str,
                    sequence_str,
                    quality_scores_str,
                };

                global_state.stream << record;
            }
        }
    };

    static void FastqWriteCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &lstate)
    {
    }

    void FastqWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate)
    {
        auto &global_state = (FastqWriteGlobalState &)gstate;

        global_state.stream << klibpp::kend;
        close(global_state.file_descriptor);
    };

    static unique_ptr<FunctionData> FastqCopyBind(ClientContext &context, CopyInfo &info, vector<std::string> &names, vector<LogicalType> &sql_types)
    {
        auto result = make_uniq<FastqCopyBindData>();

        // Check that the input names are correct
        if (names.size() == 4)
        {
            if (names[0] != "id" || names[1] != "description" || names[2] != "sequence" || names[3] != "quality_scores")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, description, sequence)");
            }
        }
        else if (names.size() == 3)
        {
            if (names[0] != "id" || names[1] != "sequence" || names[2] != "quality_scores")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, sequence)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column names for FASTQ COPY. Expected (id, description, sequence, quality_scores) or (id, sequence, quality_scores)");
        }

        // Check the input types are correct, if 2 or 3 length is allowed and all must be varchars
        if (sql_types.size() == 3)
        {
            if (sql_types[0] != LogicalType::VARCHAR || sql_types[1] != LogicalType::VARCHAR || sql_types[2] != LogicalType::VARCHAR || sql_types[3] != LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else if (sql_types.size() == 3)
        {
            if (sql_types[0] != LogicalType::VARCHAR || sql_types[1] != LogicalType::VARCHAR || sql_types[2] != LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column types for FASTQ COPY. Expected (VARCHAR, VARCHAR, VARCHAR, VARCHAR) or (VARCHAR, VARCHAR, VARCHAR)");
        }

        result->file_name = info.file_path;
        result->in_stream = new klibpp::SeqStreamIn(result->file_name.c_str());

        return std::move(result);
    }

    CopyFunction CreateFastqCopyFunction()
    {

        CopyFunction function("fastq");

        function.copy_to_bind = FastqCopyToBind;
        function.copy_to_initialize_global = FastqWriteInitializeGlobal;
        function.copy_to_initialize_local = FastqWriteInitializeLocal;

        function.copy_to_sink = FastqWriteSink;
        function.copy_to_combine = FastqWriteCombine;
        function.copy_to_finalize = FastqWriteFinalize;

        function.copy_from_bind = FastqCopyBind;

        auto fasta_scan_function = TableFunction("read_fastq", {LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);

        function.copy_from_function = fasta_scan_function;

        function.extension = "fastq";
        return function;
    }

    CreateCopyFunctionInfo FastqIO::GetFastqCopyFunction()
    {
        auto function = CreateFastqCopyFunction();
        CreateCopyFunctionInfo info(function);

        return CreateCopyFunctionInfo(info);
    };
#endif
}

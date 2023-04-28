#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <string>
#include <vector>

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#endif

#include "fastq_io.hpp"

#include <kseq++/seqio.hpp>
#include <kseq++/kseq++.hpp>

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
#if defined(__APPLE__) || defined(__linux__)
    struct FastqCopyScanOptions
    {
    };

    struct FastqWriteBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
    };

    struct FastqWriteGlobalState : public duckdb::GlobalFunctionData
    {
        int file_descriptor;
        klibpp::KStream<int, ssize_t (*)(int __fd, const void *__buf, size_t __nbyte), klibpp::mode::Out_> stream;

        // an example of an alternative constructor
        FastqWriteGlobalState(int fd)
            : file_descriptor(fd), stream(klibpp::make_kstream(fd, write, klibpp::mode::out))
        {
        }
    };

    struct FastqCopyBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        klibpp::SeqStreamIn *in_stream;
    };

    duckdb::unique_ptr<duckdb::FunctionData>
    FastqCopyToBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastqWriteBindData>();
        result->file_name = info.file_path;

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
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
            if (type.id() != duckdb::LogicalTypeId::VARCHAR)
            {
                throw std::runtime_error("Invalid column type for FASTA file, expected VARCHAR.");
            }
        }

        return std::move(result);
    };

    static duckdb::unique_ptr<duckdb::GlobalFunctionData> FastqWriteInitializeGlobal(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, const std::string &file_path)
    {
        auto &fasta_write_bind = (FastqWriteBindData &)bind_data;
        auto file_name = fasta_write_bind.file_name;

        auto compression = duckdb::StringUtil::EndsWith(file_name, ".gz");

        auto file_pointer = open(file_name.c_str(), O_WRONLY | O_CREAT, 0644);

        auto global_state = duckdb::make_unique<FastqWriteGlobalState>(file_pointer);

        return std::move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> FastqWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return std::move(local_data);
    }

    static void FastqWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                               duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
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

    static void FastqWriteCombine(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate, duckdb::LocalFunctionData &lstate)
    {
    }

    void FastqWriteFinalize(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate)
    {
        auto &global_state = (FastqWriteGlobalState &)gstate;

        global_state.stream << klibpp::kend;
        close(global_state.file_descriptor);
    };

    static duckdb::unique_ptr<duckdb::FunctionData> FastqCopyBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastqCopyBindData>();

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
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR || sql_types[2] != duckdb::LogicalType::VARCHAR || sql_types[3] != duckdb::LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else if (sql_types.size() == 3)
        {
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR || sql_types[2] != duckdb::LogicalType::VARCHAR)
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

    duckdb::CopyFunction CreateFastqCopyFunction()
    {

        duckdb::CopyFunction function("fastq");

        function.copy_to_bind = FastqCopyToBind;
        function.copy_to_initialize_global = FastqWriteInitializeGlobal;
        function.copy_to_initialize_local = FastqWriteInitializeLocal;

        function.copy_to_sink = FastqWriteSink;
        function.copy_to_combine = FastqWriteCombine;
        function.copy_to_finalize = FastqWriteFinalize;

        function.copy_from_bind = FastqCopyBind;

        auto fasta_scan_function = duckdb::TableFunction("read_fastq", {duckdb::LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);

        function.copy_from_function = fasta_scan_function;

        function.extension = "fastq";
        return function;
    }

    duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> FastqIO::GetFastqCopyFunction()
    {
        auto function = CreateFastqCopyFunction();
        duckdb::CreateCopyFunctionInfo info(function);

        return duckdb::make_unique<duckdb::CreateCopyFunctionInfo>(info);
    };
#endif
}

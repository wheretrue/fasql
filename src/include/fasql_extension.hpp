#pragma once

#include "duckdb.hpp"

namespace duckdb
{

	class FasqlExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
	};

}

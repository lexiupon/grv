// Trusted test initialization, separate from the production adapter executable.
#include "duckdb.hpp"
#include <iostream>
using namespace duckdb;
int main(int argc,char **argv) {
    try {
        if (argc != 2 && argc != 3) {return 2;}
        DuckDB database(argv[1]);Connection connection(database);
        auto execute = [&](const char *query) {auto result=connection.Query(query);if(result->HasError()){throw std::runtime_error(result->GetError());}};
        execute("CREATE TABLE first AS SELECT range::INTEGER AS id, range::BIGINT AS rowid, 'row-' || range::VARCHAR AS label, range % 2 = 0 AS selected FROM range(5000)");
        execute("CREATE TABLE empty(id BIGINT, label VARCHAR)");
        execute("CREATE VIEW forbidden_view AS SELECT * FROM first");
        execute("CREATE TABLE typed AS SELECT true AS b, -127::TINYINT AS i, '-0'::FLOAT AS f, 1.25::DOUBLE AS d, '🍕'::VARCHAR AS s, '\\x00\\xFF'::BLOB AS blob, DATE '1970-01-02' AS date, 1234567890123456789012345678.12::DECIMAL(30,2) AS decimal, TIMESTAMP_S '1970-01-01 00:00:01' AS sec, TIMESTAMP_MS '1970-01-01 00:00:00.001' AS ms, TIMESTAMP '1970-01-01 00:00:00.000001' AS us, TIMESTAMP_NS '1970-01-01 00:00:00.000000001' AS ns, TIMESTAMPTZ '1970-01-01 00:00:00+00' AS utc");
        if (argc == 3) {
            if (string(argv[2]) == "large") {execute("CREATE TABLE large AS SELECT repeat('x',5242880) AS payload");}
            execute("CREATE SCHEMA _grv");
            if (string(argv[2]) != "unknown") {
                execute("CREATE TABLE _grv.metadata_header(version BIGINT, ownership_version BIGINT); INSERT INTO _grv.metadata_header VALUES (1,1)");
                execute("CREATE TABLE _grv.relation_ownership(schema_name VARCHAR,table_name VARCHAR,kind VARCHAR)");
                if (string(argv[2]) == "managed") {execute("INSERT INTO _grv.relation_ownership VALUES ('main','first','managed_import')");}
            }
        }
        return 0;
    } catch (const std::exception &error) {std::cerr<<error.what()<<'\n';return 1;}
}

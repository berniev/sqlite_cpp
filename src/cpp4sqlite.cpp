//
// Copyright 2024 by Berniev.
//

#include "cpp4sqlite.h"

using namespace cpp4sqlite;

//--------------------------------------------------------------------------------------------------

Connection::Connection(std::string_view const name, OpenOption flags, char const* vfs)
{
    if (auto const res{sqlite3_open_v2(name.data(), &sqliteDb, static_cast<int>(flags), vfs)}) {
        /**/
        auto const err{sqlite3_errstr(res)};
        close();
        throw std::runtime_error(std::string{"Connection: misc error: "} + err);
    }
}

void Connection::close() const
{
    sqlite3_close(sqliteDb);
}

Connection::~Connection()
{
    close();
}

std::string Connection::errorStr() const
{
    return {sqlite3_errmsg(sqliteDb)};
}

int Connection::affectedRows() const
{
    return sqlite3_changes(sqliteDb);
}

SqlTable Connection::quickQuery(std::string const& queryStr)
{
    results.clear(); // updated by callback
    char* error;
    sqlite3_exec(sqliteDb, queryStr.c_str(), &callback, this, &error);
    errorMsg = fixNullStr(error);
    if (!errorMsg.empty()) {
        throw std::runtime_error("Connection::QuickQuery error: " + errorMsg);
    }
    return results;
}

int Connection::processSqlite3Callback(int const count, char** values, char** names)
{
    SqlRow row{};
    for (int i = 0; i < count; ++i) {
        row.emplace_back(names[i], fixNullStr(values[i]));
    }
    results.push_back(row);
    return 0;
}

PreparedStatement Connection::prepare(std::string const& queryStr, int const prepFlags) const
{
    sqlite3_stmt* stmnt;
    char const* data = queryStr.data();
    int const size = static_cast<int>(queryStr.size());
    char const* unused;
    if (int const res = sqlite3_prepare_v3(sqliteDb, data, size, prepFlags, &stmnt, &unused)) {
        throw std::runtime_error(std::string{"Prepare error: "} + std::to_string(res) + " : "
            + errorStr());
    }
    PreparedStatement pStmnt{stmnt};
    return pStmnt;
}

int Connection::lastInsertId() const
{
    return static_cast<int>(sqlite3_last_insert_rowid(sqliteDb));
}

bool Connection::getAutocommit() const
{
    return sqlite3_get_autocommit(sqliteDb) > 0;
}

//--------------------------------------------------------------------------------------------------

Binder::Binder(sqlite3_stmt* stmnt)
    : stmnt{stmnt}
{
}

void Binder::reset()
{
    if (int const res{sqlite3_reset(stmnt)}) {
        throw std::runtime_error(std::string{"Binder::reset error: "} + errString(res));
    }
    bindPosn = 0;
}

void Binder::checkBindParamCount(std::size_t const size) const
{
    if (std::size_t const count{static_cast<std::size_t>(sqlite3_bind_parameter_count(stmnt))}; size
        != count) {
        throw std::runtime_error(size > count ? "too many bind params" : "too few bind params");
    }
}

void Binder::bindInt(int const param) const
{
    checkResult(sqlite3_bind_int(stmnt, bindPosn, param));
}

void Binder::bindDouble(double const param) const
{
    checkResult(sqlite3_bind_double(stmnt, bindPosn, param));
}

void Binder::bindText(char const* param) const
{
    checkResult(sqlite3_bind_text(stmnt, bindPosn, param, -1, SQLITE_TRANSIENT));
}

void Binder::bindBlob(std::string const& param) const
{
    auto const data = reinterpret_cast<const unsigned char*>(param.data());
    auto const size = static_cast<int>(param.size());
    checkResult(sqlite3_bind_blob(stmnt, bindPosn, &data[0], size, SQLITE_TRANSIENT));
}

// ReSharper disable once CppDFAUnreachableFunctionCall
void Binder::checkResult(int const res) const
{
    if (res) {
        std::string const err{"bind fail at posn "};
        throw std::runtime_error(err + std::to_string(bindPosn));
    }
}

//--------------------------------------------------------------------------------------------------

ResultColumn::ResultColumn(sqlite3_stmt* stmnt, int const posn)
    : stmnt{stmnt}
      , posn{posn}
      , type{sqlite3_column_type(stmnt, posn)}
{
}

SqlColName ResultColumn::name() const
{
    return sqlite3_column_name(stmnt, posn);
}

std::string ResultColumn::readText() const
{
    auto const text = reinterpret_cast<const char*>(sqlite3_column_text(stmnt, posn));
    auto const size = static_cast<std::size_t>(sqlite3_column_bytes(stmnt, posn));
    return {text, size};
}

std::string ResultColumn::readBlob() const
{
    auto data = static_cast<const unsigned char*>(sqlite3_column_blob(stmnt, posn));
    auto const size = sqlite3_column_bytes(stmnt, posn);
    return {data, data + size};
}

SqlField ResultColumn::field() const
{
    return {name(), fieldS()};
}

std::string ResultColumn::fieldS() const
{
    return readText();
}

//--------------------------------------------------------------------------------------------------

Resultset::Resultset(sqlite3_stmt* stmnt)
    : stmnt{stmnt}
{
    step();
    int const count = countColumns();
    for (int i = 0; i < count; ++i) {
        columns.emplace_back(stmnt, i);
    }
}

void Resultset::step()
{
    switch (int const res{sqlite3_step(stmnt)}) {

        case SQLITE_DONE:
            hasRow = false;
            break;

        case SQLITE_ROW:
            hasRow = true;
            columnPosn = 0;
            break;

        default:
            throw std::runtime_error(std::string{"Resultset::step error: " + std::to_string(res)});
    }
}

int Resultset::countColumns() const
{
    return sqlite3_column_count(stmnt);
}

int Resultset::countData() const
{
    return sqlite3_data_count(stmnt);
}

int Resultset::posn(SqlColName const& name) const
{
    for (auto const& column : columns) {
        if (column.name() == name) {
            return static_cast<int>(&column - &columns[0]);
        }
    }

    throw std::runtime_error(name + " col name not found");
}

SqlField Resultset::field(int const posn) const
{
    if (!hasRow) {
        return {};
    }
    return columns.at(posn).field();
}

std::string Resultset::fieldS(int const posn) const
{
    if (!hasRow) {
        return {};
    }
    return columns.at(posn).fieldS();
}

std::string Resultset::fieldS(std::string const& name) const
{
    return fieldS(posn(name));
}

std::string Resultset::nextFieldS()
{
    return fieldS(++columnPosn);
}

SqlField Resultset::field(std::string const& name) const
{
    return columns.at(posn(name)).field();
}

SqlField Resultset::nextField()
{
    return field(++columnPosn);
}

std::optional<SqlRow> Resultset::row()
{
    if (!hasRow) {
        return {};
    }

    SqlRow row{};
    int const colCount = countData();
    for (int i{0}; i < colCount; ++i) {
        row.push_back(field(i));
    }
    step();
    return {row};
}

std::optional<SqlRowS> Resultset::rowS()
{
    if (!hasRow) {
        return {};
    }

    SqlRowS rowS{};
    for (int i{0}; i < countData(); ++i) {
        rowS.push_back(fieldS(i));
    }
    step();
    return {rowS};
}

bool Resultset::empty() const
{
    return !hasRow;
}

int Resultset::toFile(std::filesystem::path const& fileSpec, FileReplace const replace) const
{
    if (exists(fileSpec)) {
        if (replace != FileReplace::yes) {
            throw std::runtime_error(std::string{"File already exists: "} + fileSpec.string());
        }

        // delete file, or will file.write overwrite it?
    }
    std::ofstream file(fileSpec, std::ios::binary);
    void const* blob = sqlite3_column_blob(stmnt, 0);
    int const size = sqlite3_column_bytes(stmnt, 0);
    file.write(static_cast<char const*>(blob), size);
    return size;
}

void Resultset::checkTypeCount(int const count) const
{
    if (auto const colCount = countData(); colCount != count) {
        throw std::runtime_error(count > colCount ? "too many types" : "too few types");
    }
}

//--------------------------------------------------------------------------------------------------

PreparedStatement::PreparedStatement(sqlite3_stmt* stmnt)
    : stmnt{stmnt}
{
}

PreparedStatement::~PreparedStatement()
{
    sqlite3_finalize(stmnt);
}

//--------------------------------------------------------------------------------------------------
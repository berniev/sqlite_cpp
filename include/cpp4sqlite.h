/*
 Copyright (c) 2024 Berniev

 Licence: MIT
 */

#ifndef SQLITE_CPP_H
#define SQLITE_CPP_H


#include <sqlite3.h>
#include <string>
#include <fstream>

namespace cpp4sqlite
{

//--------------------------------------------------------------------------------------------------

using SqlColName = std::string;
using SqlColNames = std::vector<SqlColName>;
using SqlField = std::pair<SqlColName, std::string>;
using SqlFieldS = std::string;
using SqlRow = std::vector<SqlField>;
using SqlRowS = std::vector<std::string>;
using SqlTable = std::vector<SqlRow>;

//--------------------------------------------------------------------------------------------------

enum class OpenOption
{  // clang-format off
    READONLY     = 0x00000001,
    READWRITE    = 0x00000002,
    CREATE       = 0x00000004,
    CREATERW     = 0x00000006,
    URI          = 0x00000008,
    MEMORY       = 0x00000010,
    NOMUTEX      = 0x00008000,
    FULLMUTEX    = 0x00010000,
    SHAREDCACHE  = 0x00020000,
    PRIVATECACHE = 0x00040000,
    NOFOLLOW     = 0x01000000,
    EXRESCODE    = 0x02000000
};  // clang-format on

//--------------------------------------------------------------------------------------------------

inline char const* fixNullStr(char const* str)
{
    return str == nullptr ? "" : str;
}

//--------------------------------------------------------------------------------------------------

class PreparedStatement;

class Connection
{
    sqlite3* sqliteDb {};
    std::string errorMsg {};
    SqlTable results {};

public:
    explicit
    Connection(std::string_view name, OpenOption = OpenOption::READONLY, char const* vfs = nullptr);
    ~Connection();
    Connection() = delete;
    Connection(Connection&) = delete;
    Connection(Connection&&) noexcept = delete;
    Connection& operator=(Connection&) = delete;
    Connection& operator=(Connection&&) = delete;

    /**
     * Quick Query
     */
    SqlTable quickQuery(std::string const& queryStr);
    int processSqlite3Callback(int count, char** values, char** names);

    /**
     * Prepared Statement
     */
    [[nodiscard]] PreparedStatement prepare(std::string const& queryStr, int prepFlags = 0) const;

    [[nodiscard]] std::string errorStr() const;
    [[nodiscard]] int affectedRows() const;
    [[nodiscard]] int lastInsertId() const;

    /**
     * If certain errors occur on a statement within a multi-statement transaction
     * (including SQLITE_FULL, SQLITE_IOERR, SQLITE_NOMEM, SQLITE_BUSY, and SQLITE_INTERRUPT)
     * then the transaction might be rolled back automatically.
     * This is the only way to find out whether SQLite automatically rolled back the transaction.
     */
    [[nodiscard]] bool getAutocommit() const;

private:
    void close() const;
};

//--------------------------------------------------------------------------------------------------

/**
 * fires each time quickQuery generates a resultset row
 */
inline int callback(void* host, int const numFields, char** values, char** names)
{
    return static_cast<Connection*>(host)->processSqlite3Callback(numFields, values, names);
}

inline std::string errString(int const errornum)
{
    char const* errStr = sqlite3_errstr(errornum);
    return {fixNullStr(errStr)};
}

//--------------------------------------------------------------------------------------------------

class Resultset;

class Binder
{
    int bindPosn {0};  // 1-indexed
    sqlite3_stmt* stmnt {};

public:
    explicit Binder(sqlite3_stmt* stmnt);

    /**
     * Set parameters of prepared statement
     */
    template<typename... Types>
    void setParams(Types... values)
    {
        checkBindParamCount(sizeof...(Types));
        reset();
        auto doBinds = [&](auto&&... arg) {
            (bind(arg), ...);
        };
        std::apply(doBinds, std::tuple<Types...> {values...});
    }

private:
    template<typename T>
    void bind(T& param)
    {
        ++bindPosn;

        if constexpr (std::is_same_v<T, int>) {
            bindInt(param);
        }

        else if constexpr (std::is_same_v<T, double>) {
            bindDouble(param);
        }

        else if constexpr (std::is_same_v<T, char const*>) {
            bindText(param);
        }

        else if constexpr (std::is_same_v<T, std::string>) {
            auto const data = param.data();
            if (strlen(param.data()) == param.size()) {
                {
                    bindText(data);
                }
            }
            else {
                bindBlob(param);
            }
        }

        else if constexpr (std::is_same_v<T, std::filesystem::path>) {
            std::ifstream stream {param, std::ios::binary | std::ios::ate};
            if (!stream) {
                throw std::runtime_error(std::string {"Invalid filePath: "} + param.string());
            }

            auto const fsize {stream.tellg()};
            std::string str(fsize, '\0');
            stream.seekg(0);
            if (!stream.read(&str[0], fsize)) {
                return;
            }

            bindBlob(str);
        }

        else if constexpr (std::is_same_v<T, std::nullptr_t>) {
            checkResult(sqlite3_bind_null(stmnt, bindPosn));
        }
    }

    void bindInt(int param) const;
    void bindDouble(double param) const;
    void bindText(char const* param) const;
    void bindBlob(std::string const& param) const;

    void reset();
    void checkBindParamCount(std::size_t size) const;
    void checkResult(int res) const;
};

//--------------------------------------------------------------------------------------------------

class ResultColumn
{
    sqlite3_stmt* stmnt {};
    int posn {};
    int type {};  // 1 SQLITE_INTEGER, 2 SQLITE_FLOAT, 3 SQLITE_TEXT, 4 SQLITE_BLOB, 5 SQLITE_NULL


public:
    ResultColumn(sqlite3_stmt* stmnt, int posn);

    [[nodiscard]] SqlColName name() const;

    [[nodiscard]] SqlField field() const;
    [[nodiscard]] std::string fieldS() const;

    template<typename T>
    std::optional<T> read()
    {
        if (type == SQLITE_NULL) {
            return {};
        }

        if constexpr (std::is_same_v<T, int>) {
            return sqlite3_column_int(stmnt, posn);
        }
        if constexpr (std::is_same_v<T, long long>) {
            return sqlite3_column_int64(stmnt, posn);
        }
        else if constexpr (std::is_same_v<T, double>) {
            return sqlite3_column_double(stmnt, posn);
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            return type == SQLITE_BLOB ? readBlob() : readText();
        }

        const std::string emsg {"Read: Unrecognised read type requested"};
        throw std::runtime_error(emsg + " for `" + name() + "`");
    }

private:
    [[nodiscard]] std::string readText() const;
    [[nodiscard]] std::string readBlob() const;
};

//--------------------------------------------------------------------------------------------------

class Resultset
{
    sqlite3_stmt* stmnt {};
    bool hasRow {false};
    int columnPosn {0};
    std::vector<ResultColumn> columns {};

public:
    enum class FileReplace
    {
        no,
        yes
    };

    explicit Resultset(sqlite3_stmt* stmnt);

    [[nodiscard]] int countColumns() const;
    [[nodiscard]] int countData() const;
    [[nodiscard]] bool empty() const;

    [[nodiscard]] SqlField
    field(int posn = 0) const;  // field (name/value pair) at position (0-based)
    [[nodiscard]] SqlField field(SqlColName const& name) const;  // named field
    SqlField nextField();                                        // next field

    [[nodiscard]] std::string fieldS(int posn = 0) const;
    [[nodiscard]] std::string fieldS(SqlColName const& name) const;
    std::string nextFieldS();

    std::optional<SqlRow> row();
    std::optional<SqlRowS> rowS();

    template<typename T>
    std::optional<T> fieldT()
    {
        return columns.at(columnPosn).read<T>();
    }

    template<typename T>
    std::optional<T> fieldT(int const posn)
    {
        if (!hasRow) {
            return {};
        }
        return columns.at(posn).read<T>();
    }

    template<typename T>
    std::optional<T> fieldT(SqlColName const& name)
    {
        return fieldT<T>(posn(name));
    }

    template<typename T>
    std::optional<T> nextFieldT()
    {
        ++columnPosn;
        return fieldT<T>();
    }

    template<typename... T>
    std::optional<std::tuple<std::optional<T>...>> rowT()
    {
        if (!hasRow) {
            return {};
        }

        auto incPos = [this] {
            return columnPosn++;
        };

        checkTypeCount(std::tuple_size_v<std::tuple<T...>>);

        auto tup = std::make_tuple(fieldT<T>(incPos())...);
        step();
        return {tup};
    }

    [[nodiscard]] int toFile(std::filesystem::path const& fileSpec,
                             FileReplace replace = FileReplace::no) const;

private:
    void checkTypeCount(int count) const;
    void step();
    [[nodiscard]] int posn(SqlColName const& name) const;
};

//--------------------------------------------------------------------------------------------------

class PreparedStatement
{
    sqlite3_stmt* stmnt {};

public:
    explicit PreparedStatement(sqlite3_stmt* stmnt);
    ~PreparedStatement();
    // rule of 5
    PreparedStatement() = delete;
    PreparedStatement(PreparedStatement&) = delete;
    PreparedStatement(PreparedStatement&&) noexcept = default;
    PreparedStatement& operator=(PreparedStatement&) = delete;
    PreparedStatement& operator=(PreparedStatement&&) = delete;

    template<typename... Types>
    Resultset execute(Types... values)
    {
        Binder {stmnt}.setParams(values...);

        Resultset res {stmnt};
        return res;
    }
};

//--------------------------------------------------------------------------------------------------

}  // namespace sqlite_cpp
#endif  // SQLITE_CPP_H

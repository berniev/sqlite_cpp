/*
 Copyright (c) 2024 Berniev

 Licence: MIT
 */

#include <gtest/gtest.h>
#include <cpp4sqlite.h>
#include <memory>

using namespace cpp4sqlite;

constexpr auto databasePath {":memory:"};
constexpr auto openOption {OpenOption::READWRITE};

constexpr auto createQueryStr {R"(
    CREATE TABLE Test(
        text_col_key text not null
                   constraint config_pk
                   primary key,
        text_col    text,
        int_col     integer,
        float_col   real,
        blob_col    blob
    );

    INSERT INTO Test ( text_col_key, text_col, int_col, float_col, blob_col )
              VALUES ( 'row11'     , 'one'   , '1'    , '1.1'    , NULL     ),
                     ( 'row21'     , 'two'   , '2'    , '2.2'    , NULL     ),
                     ( 'row31'     , '€tre'  , '3'    , '3.3'    , NULL     ),
                     ( 'row41'     , 'for'   , '4'    , '4.4'    , NULL     ),
                     ( 'row42'     , 'for'   , '4'    , '4.4'    , NULL     ),
                     ( 'row51'     , '51'    , '51'   , '5.5'    , NULL     ),
                     ( 'row91'     , 'nin'   ,  NULL  ,  NULL    , NULL     )
)"};

class SqlTests: public testing::Test
{
protected:
    static std::unique_ptr<Connection> connection;
    static void SetUpTestSuite()
    {
        connection->quickQuery(createQueryStr);
    }
};

std::unique_ptr<Connection> SqlTests::connection =
    std::make_unique<Connection>(databasePath, openOption);

//--------------------------------------------------------------------------------------------------

TEST_F(SqlTests, field_default)
{
    auto const result =
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = '4'").execute().field();
    SqlField const expect {"text_col_key", "row41"};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, field_number)
{
    auto const result =
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = '4'").execute().field(0);
    SqlField const expect {"text_col_key", "row41"};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, field_name)
{
    auto const result = connection->prepare("SELECT text_col_key FROM Test WHERE int_col ='4'")
                            .execute()
                            .field("text_col_key");
    SqlField const expect {"text_col_key", "row41"};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, nextField)
{
    auto const result =
        connection->prepare("SELECT text_col_key, text_col FROM Test WHERE int_col = '4'")
            .execute()
            .nextField();
    SqlField const expect {"text_col", "for"};
    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, nextField_no_next_field)
{
    auto test = [] {
        auto statement = connection->prepare("SELECT text_col_key FROM Test WHERE int_col = '4'")
                             .execute()
                             .nextField();
    };
    EXPECT_THROW(test(), std::exception);
}

TEST_F(SqlTests, fieldS)
{
    auto const result =
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?").execute(4).fieldS();
    EXPECT_EQ("row41", result);
}

TEST_F(SqlTests, fieldS_UTF_8)
{
    auto const result =
        connection->prepare("SELECT text_col FROM Test WHERE int_col = ?").execute(3).fieldS();
    EXPECT_EQ("€tre", result);
}

TEST_F(SqlTests, nextFieldS)
{
    auto const result =
        connection->prepare("SELECT text_col_key, text_col FROM Test WHERE int_col = ?")
            .execute(4)
            .nextFieldS();
    EXPECT_EQ("for", result);
}

TEST_F(SqlTests, fieldT_string)
{
    auto const result = connection->prepare("SELECT text_col_key FROM Test WHERE int_col =?")
                            .execute(4)
                            .fieldT<std::string>();

    EXPECT_EQ("row41", result);
}

TEST_F(SqlTests, fieldT_const_char)
{
    auto test = [] {
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?")
            .execute(4)
            .fieldT<char const*>();
    };

    EXPECT_THROW(test(), std::exception);  // "const char* not supported for read type";
}

TEST_F(SqlTests, fieldT_int)
{
    auto const result = connection->prepare("SELECT int_col FROM Test WHERE text_col =?")
                            .execute("for")
                            .fieldT<int>();

    EXPECT_EQ(4, result);
}

TEST_F(SqlTests, fieldT_long_long)
{
    auto const result = connection->prepare("SELECT int_col FROM Test WHERE text_col =?")
                            .execute("for")
                            .fieldT<long long>();

    EXPECT_EQ(4, result);
}

TEST_F(SqlTests, fieldT_double)
{
    auto const result = connection->prepare("SELECT float_col FROM Test WHERE text_col =?")
                            .execute("for")
                            .fieldT<double>();

    EXPECT_EQ(4.4, result);
}

TEST_F(SqlTests, fieldT_unrecognised_type)
{
    auto test = [] {
        connection->prepare("SELECT float_col FROM Test WHERE text_col = ?")
            .execute("for")
            .fieldT<std::vector<int>>();
    };

    EXPECT_THROW(test(),
                 std::runtime_error);  // "Read: Unrecognised read type requested for `float_col`"
}

TEST_F(SqlTests, fieldT_and_nextFieldT)
{
    auto statement = connection->prepare("SELECT * FROM Test WHERE int_col = ?");
    auto resultSet = statement.execute(4);

    EXPECT_EQ("for", resultSet.nextFieldT<std::string>());
    EXPECT_EQ(4, resultSet.nextFieldT<int>());
    EXPECT_EQ(4.4, resultSet.nextFieldT<double>());
}

TEST_F(SqlTests, fieldT_null_is_nullopt)
{
    auto const result = connection->prepare("SELECT int_col FROM Test WHERE text_col =?")
                            .execute("nin")
                            .fieldT<int>();

    EXPECT_FALSE(result.has_value());
}

TEST_F(SqlTests, fieldT_null_value_or)
{
    auto const result = connection->prepare("SELECT int_col FROM Test WHERE text_col =?")
                            .execute("nin")
                            .fieldT<int>()
                            .value_or(99999);
    EXPECT_EQ(99999, result);
}

TEST_F(SqlTests, row)
{
    auto const actual = connection
                            ->prepare("SELECT text_col_key, text_col, float_col, blob_col FROM "
                                      "Test WHERE text_col_key = ?")
                            .execute("row31")
                            .row();
    SqlRow const expect {
        {{"text_col_key", "row31"}, {"text_col", "€tre"}, {"float_col", "3.3"}, {"blob_col", ""}}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, rowS)
{
    auto statement = connection->prepare(
        "SELECT text_col_key, text_col, int_col, float_col, blob_col FROM Test WHERE int_col = ?");
    auto resultSet = statement.execute(4);

    auto const result1 = resultSet.rowS();
    SqlRowS const expect1 = {"row41", "for", "4", "4.4", ""};

    EXPECT_EQ(expect1, result1);

    auto const result2 = resultSet.rowS();
    SqlRowS const expect2 = {"row42", "for", "4", "4.4", ""};

    EXPECT_EQ(expect2, result2);
}

TEST_F(SqlTests, rowS_nulls_converted_to_empty_string)
{
    auto const result = connection
                            ->prepare(R"(
        SELECT text_col, int_col, float_col, blob_col
        FROM Test
        WHERE int_col IS NULL
    )")
                            .execute()
                            .rowS();
    SqlRowS const expect = {"nin", "", "", ""};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, rowT)
{
    auto [key, str, intVal, doubleVal] =
        connection
            ->prepare(
                "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ?")
            .execute(2)
            .rowT<std::string, std::string, int, double>()
            .value();

    EXPECT_EQ("row21", key.value());
    EXPECT_EQ("two", str.value());
    EXPECT_EQ(2, intVal.value());
    EXPECT_EQ(2.2, doubleVal.value());
}

TEST_F(SqlTests, rowT_without_value_calls_where_optional_has_value)
{
    auto [key, str, intVal, doubleVal] =
        connection
            ->prepare(
                "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ?")
            .execute(2)
            .rowT<std::string, std::string, int, double>()
            .value();

    EXPECT_EQ("row21", key);
    EXPECT_EQ("two", str);
    EXPECT_EQ(2, intVal);
    EXPECT_EQ(2.2, doubleVal);
}

TEST_F(SqlTests, rowT_value_or)
{
    auto [key, str, intVal, doubleVal] =
        connection
            ->prepare(
                "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE text_col = ?")
            .execute("nin")
            .rowT<std::string, std::string, int, double>()
            .value();

    EXPECT_EQ("row91", key.value_or("NULL"));
    EXPECT_EQ("nin", str.value_or("NULL"));
    EXPECT_EQ(0, intVal.value_or(0));
    EXPECT_EQ(0, doubleVal.value_or(0));
}

TEST_F(SqlTests, rowT_nullopt)
{
    auto [intVal, doubleVal] =
        connection->prepare("SELECT int_col, float_col FROM Test WHERE text_col = ?")
            .execute("nin")
            .rowT<int, double>()
            .value();

    EXPECT_EQ(std::nullopt, intVal);
    EXPECT_EQ(std::nullopt, doubleVal);
}

TEST_F(SqlTests, rowT_wrong_types_throws)
{
    auto test = [] {
        connection
            ->prepare(
                "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ?")
            .execute(4)
            .rowT<std::string, int, int, float>();
    };

    EXPECT_THROW(test(), std::runtime_error);
}

TEST_F(SqlTests, rowT_too_few_types_throws)
{
    auto test = [] {
        connection
            ->prepare(
                "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ?")
            .execute(4)
            .rowT<std::string>();
    };

    EXPECT_THROW(test(), std::runtime_error);
}

TEST_F(SqlTests, rowT_too_many_types_throws)
{
    auto test = [&] {
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?")
            .execute(4)
            .rowT<std::string, std::string, int, double, long>();
    };

    EXPECT_THROW(test(), std::runtime_error);
}

TEST_F(SqlTests, rowT_has_value_false_no_result)
{
    auto test = [&] {
        return connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?")
            .execute(4321)
            .rowT<std::string, std::string, int, double, long>();
    };

    EXPECT_FALSE(test().has_value());
}

TEST_F(SqlTests, invalid_query_throws)
{
    auto test = [&] {
        return connection->prepare("SEL * FROM Test");  // should be "SELECT"
    };

    EXPECT_THROW(test(), std::runtime_error);
}

TEST_F(SqlTests, single_row_one_column_nextField)
{
    auto test = [] {
        auto const actual = connection->prepare("SELECT text_col FROM Test WHERE int_col ='4'")
                                .execute()
                                .nextField();
    };
    SqlField const expect {"text_col", "for"};

    EXPECT_THROW(test(), std::out_of_range);
}

TEST_F(SqlTests, single_row_one_column_numbered_field)
{
    auto const actual =
        connection->prepare("SELECT * FROM Test WHERE int_col = '4'").execute().field(3);
    SqlField const expect {"float_col", "4.4"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, single_row_one_column_named_field)
{
    auto const actual = connection->prepare("SELECT text_col FROM Test WHERE text_col_key =?")
                            .execute("row21")
                            .field("text_col");
    SqlField const expect {"text_col", "two"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, rows_subset_single_row_row)
{
    auto const actual =
        connection->prepare("SELECT text_col_key, text_col FROM Test WHERE text_col_key = ?")
            .execute("row31")
            .row();
    SqlRow const expect {{"text_col_key", "row31"}, {"text_col", "€tre"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, empty_actual)
{
    auto const actual =
        connection->prepare("SELECT text_col_key, text_col FROM Test WHERE text_col_key = 'xx'")
            .execute();

    EXPECT_TRUE(actual.empty());
}

TEST_F(SqlTests, too_many_binds_throws)
{
    auto statement = connection->prepare(R"(
        SELECT text_col_key, text_col, int_col, float_col
        FROM Test
        WHERE int_col > ? AND int_col < ?
    )");

    EXPECT_THROW(statement.execute(3, 5, 7), std::runtime_error);
}

TEST_F(SqlTests, too_few_binds_throws)
{
    auto statement = connection->prepare(R"(
        SELECT text_col_key, text_col, int_col, float_col
        FROM Test
        WHERE int_col > ? AND int_col < ?
    )");

    EXPECT_THROW(statement.execute(3), std::runtime_error);
}

TEST_F(SqlTests, incorrect_bind_type_doesnt_throw)
{
    auto statement = connection->prepare("SELECT text_col_key FROM Test  WHERE int_col = ?");

    EXPECT_NO_THROW(statement.execute("Test"));
}

TEST_F(SqlTests, bind_one_int_successive_row)
{
    auto statement = connection->prepare(
        "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ?");
    auto resultSet = statement.execute(4);
    std::optional<SqlRow> const actual = resultSet.row();

    EXPECT_TRUE(actual);

    SqlRow const expect {{"text_col_key", "row41"},
                         {"text_col", "for"},
                         {"int_col", "4"},
                         {"float_col", "4.4"}};

    EXPECT_EQ(expect, actual);

    auto const actual2 = resultSet.row();
    SqlRow const expect2 {{"text_col_key", "row42"},
                          {"text_col", "for"},
                          {"int_col", "4"},
                          {"float_col", "4.4"}};

    EXPECT_EQ(expect2, actual2);
}

TEST_F(SqlTests, bind_two_ints)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key, text_col, int_col, float_col
        FROM Test
        WHERE int_col > ? AND int_col < ?
    )")
                            .execute(3, 5)
                            .row();

    SqlRow const expect {{"text_col_key", "row41"},
                         {"text_col", "for"},
                         {"int_col", "4"},
                         {"float_col", "4.4"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_one_std_string)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key
        FROM Test
        WHERE text_col = ?
    )")
                            .execute(std::string {"one"})
                            .field();

    SqlField const expect {"text_col_key", "row11"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_one_string_on_int)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key
        FROM Test
        WHERE int_col = ?
    )")
                            .execute(std::string {"1"})
                            .field();

    SqlField const expect {"text_col_key", "row11"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_one_int_on_string)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key
        FROM Test
        WHERE text_col = ?
    )")
                            .execute(51)
                            .field();

    SqlField const expect {"text_col_key", "row51"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_mixed_types)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key, text_col, int_col, float_col
        FROM Test
        WHERE text_col = ? AND int_col = ? AND float_col > ?
    )")
                            .execute("for", 4, 4.3)
                            .row()
                            .value();
    SqlRow const expect {{"text_col_key", "row41"},
                         {"text_col", "for"},
                         {"int_col", "4"},
                         {"float_col", "4.4"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_one_string_row_non_text_col_key_empty_table)
{
    auto const actual =
        connection->prepare("SELECT text_col_key FROM Test WHERE float_col > ?").execute(44).row();

    EXPECT_FALSE(actual);
}

TEST_F(SqlTests, bind_one_string_empty_row_non_text_col_key)
{
    auto const actual =
        connection->prepare("SELECT text_col_key FROM Test WHERE float_col > ?").execute(44);

    EXPECT_TRUE(actual.empty());
}

TEST_F(SqlTests, bind_one_int)
{
    auto const actual =
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?").execute(44);

    EXPECT_TRUE(actual.empty());
}

TEST_F(SqlTests, bind_one_float)
{
    auto const actual =
        connection->prepare("SELECT text_col_key FROM Test WHERE float_col > ?").execute(44.0);

    EXPECT_TRUE(actual.empty());
}

TEST_F(SqlTests, bind_one_string_empty_row)
{
    auto const actual = connection->prepare("SELECT text_col_key FROM Test WHERE text_col =?")
                            .execute("row41aa")
                            .rowS();

    EXPECT_FALSE(actual);
    EXPECT_FALSE(actual.has_value());
}

TEST_F(SqlTests, bind_one_string_row_non_text_col_key_unconsolidated)
{
    auto const actual = connection->prepare("SELECT text_col_key FROM Test WHERE text_col =?")
                            .execute("nin")
                            .rowS();
    SqlRowS const expect {{"row91"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, reuse_prepared_statement_with_different_params)
{
    constexpr auto queryStr {R"(
        SELECT COUNT(text_col_key)
        FROM Test
        WHERE int_col > ?
    )"};
    auto statement = connection->prepare(queryStr);

    // first use
    auto const actual1 = statement.execute(1).fieldS();

    EXPECT_EQ("5", actual1);

    // second use
    auto const actual2 = statement.execute(4).fieldT<int>();

    EXPECT_EQ(1, actual2);

    // third use
    auto const actual3 = statement.execute(3).fieldT<int>();

    EXPECT_EQ(3, actual3);
}

TEST_F(SqlTests, bind_one_int_rows_two_rows)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key,int_col
        FROM Test
        WHERE int_col = ?
    )")
                            .execute(4)
                            .row();
    SqlRow const expect {{"text_col_key", "row41"}, {"int_col", "4"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, bind_one_string_rows_non_text_col_key)
{
    auto const actual = connection
                            ->prepare(R"(
        SELECT text_col_key, int_col
        FROM Test
        WHERE int_col = ?
    )")
                            .execute(2)
                            .row();
    SqlRow const expect {{"text_col_key", "row21"}, {"int_col", "2"}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, float_field_invalid_posn_throws)
{
    auto test = [] {
        auto resultSet = connection
                             ->prepare(R"(
        SELECT float_col
        FROM Test
        WHERE int_col = ?
    )")
                             .execute(3)
                             .field(8);
    };

    EXPECT_THROW(test(), std::out_of_range);
}

TEST_F(SqlTests, chained_prepare_execute_field)
{
    auto const actual =
        connection->prepare("SELECT float_col FROM Test WHERE int_col = ?").execute(3).field();
    SqlField const expect {"float_col", "3.3"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, unchained)
{
    auto const result =
        connection->prepare("SELECT float_col FROM Test WHERE int_col = ?").execute(3).field();
    SqlField const expect {"float_col", "3.3"};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, nextField_sequential)
{
    auto statement = connection->prepare(R"(
        SELECT *
        FROM Test
        WHERE int_col = ?
    )");
    auto resultSet = statement.execute(3);
    resultSet.nextField();
    resultSet.nextField();
    auto const actual = resultSet.nextField();
    SqlField const expect {"float_col", "3.3"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, chained_prepare_execute_unchained_field)
{
    auto const result =
        connection->prepare("SELECT text_col_key FROM Test WHERE int_col = ?").execute(3).field();
    SqlField const expect {"text_col_key", "row31"};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, numbered_field_then_next)
{
    auto statement = connection->prepare(R"(
        SELECT *
        FROM Test
        WHERE text_col = '€tre'
    )");
    auto resultSet = statement.execute();
    auto field1 = resultSet.field(1);
    auto field2 = resultSet.nextField();
    auto const actual = resultSet.nextField();
    SqlField const expect {"int_col", "3"};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, insert_fail)
{
    EXPECT_THROW(connection->quickQuery("INSERT INTO Test VALUES ('row11', 'one', 1, 1.1)"),
                 std::runtime_error);
}

TEST_F(SqlTests, insert_and_delete)
{
    connection->quickQuery("INSERT INTO Test VALUES ('row61', 'son', 6, 6.6, NULL)");
    int const insertedId = connection->lastInsertId();

    EXPECT_EQ(1, connection->affectedRows()) << "inserted";

    auto const actual =
        connection->prepare("SELECT COUNT(text_col_key) AS count FROM Test WHERE ROWID = ?")
            .execute(insertedId)
            .field();

    SqlField const expect {"count", "1"};  // insert successful

    EXPECT_EQ(expect, actual) << "after insert";

    connection->prepare("DELETE FROM Test WHERE ROWID = ?").execute(insertedId);

    EXPECT_EQ(1, connection->affectedRows());  // delete successful

    auto actual3 =
        connection->prepare("SELECT COUNT(text_col_key) AS count FROM Test WHERE ROWID = ?")
            .execute(insertedId)
            .field();
    SqlField const expect3 {"count", "0"};

    EXPECT_EQ(expect3, actual3) << "after delete";
}

TEST_F(SqlTests, execute_get_results)
{
    SqlTable const actual =
        connection->quickQuery("SELECT text_col_key, text_col, int_col, float_col FROM "
                               "Test WHERE int_col = '1' OR int_col == '2'");

    SqlTable const expect {
        {{"text_col_key", "row11"}, {"text_col", "one"}, {"int_col", "1"}, {"float_col", "1.1"}},
        {{"text_col_key", "row21"}, {"text_col", "two"}, {"int_col", "2"}, {"float_col", "2.2"}}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, execute_compound_query_changes_function)
{
    SqlTable const actual = connection->quickQuery(R"(
        INSERT INTO Test VALUES ('row61', 'son', 6, 6.6, NULL),
                                ('row611', 'son', 6, 6.6, NULL);
        SELECT Changes() as changes;
        DELETE FROM Test WHERE text_col_key = 'row61' OR text_col_key = 'row611'
    )");

    SqlTable const expect {{{"changes", "2"}}};  // always a table, empty on fail

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, compound_query_changes_function)
{
    SqlTable const result = connection->quickQuery(R"(
        INSERT INTO Test VALUES ('row661', 'son', 6, 6.6, NULL),
                                ('row661x', 'son', 6, 6.6, NULL);
        SELECT Changes()
    )");
    SqlTable const expect {{{"Changes()", "2"}}};

    EXPECT_EQ(expect, result);
}

TEST_F(SqlTests, bad_compound_query_throws)
{
    auto test = [] {
        connection->quickQuery(R"(
        INSERT INTO Test
            (text_col_key, text_col, int_col, float_col)
            VALUES ('row61', 'son', 6, 6.6),
                   ('row61', 'son', 6, 6.6);
        SELECT COUNT(text_col_key) WHERE text_col_key = 'row61'
    )");
    };

    EXPECT_THROW(test(), std::runtime_error);  // UNIQUE constraint failed: Test.text_col_key
}

TEST_F(SqlTests, null_insert_delete)
{
    auto test = [] {
        connection->quickQuery("INSERT INTO Test VALUES ('row81', 'son', NULL, 8.8, NULL)");
        connection->quickQuery("DELETE FROM Test WHERE text_col_key = 'row81'");
    };

    EXPECT_NO_THROW(test());
}

TEST_F(SqlTests, null_insert_select_delete)
{
    connection->quickQuery("INSERT INTO Test VALUES ('row81', 'son', NULL, 8.8, NULL)");

    SqlTable const actual =
        connection->quickQuery("SELECT int_col FROM Test WHERE text_col_key = 'row81'");
    SqlTable const expect {{{"int_col", ""}}};

    EXPECT_EQ(expect, actual) << "after select";

    connection->quickQuery("DELETE FROM Test WHERE text_col_key = 'row81'");
}

TEST_F(SqlTests, quickQuery_selected_null_is_empty_string)
{
    SqlTable const actual = connection->quickQuery(
        "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col IS NULL");
    SqlTable const expect {
        {{"text_col_key", "row91"}, {"text_col", "nin"}, {"int_col", ""}, {"float_col", ""}}};

    EXPECT_EQ(expect, actual);
}

TEST_F(SqlTests, empty_value_is_not_same_as_null)
{
    SqlTable const actual = connection->quickQuery(
        "SELECT text_col_key, text_col, int_col, float_col FROM Test WHERE int_col = ''");

    EXPECT_TRUE(actual.empty());
}

TEST_F(SqlTests, set_NULL_using_NULL)
{
    auto statement = connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)");
    auto test = [&] {
        statement.execute("row81", "€son", 888, NULL, NULL);
    };

    EXPECT_NO_THROW(test());

    SqlTable const res = connection->quickQuery("SELECT text_col FROM Test WHERE int_col = '888'");
    SqlTable const expect = {{{"text_col", "€son"}}};

    EXPECT_EQ(res, expect);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '888'");

    ASSERT_EQ(1, connection->affectedRows());
}

TEST_F(SqlTests, set_NULL_using_nullptr)
{
    auto statement = connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)");
    auto test = [&] {
        statement.execute("row81", "€son", 888, nullptr, nullptr);
    };

    EXPECT_NO_THROW(test());

    SqlTable const res = connection->quickQuery("SELECT text_col FROM Test WHERE int_col = '888'");
    SqlTable const expect = {{{"text_col", "€son"}}};

    EXPECT_EQ(res, expect);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '888'");

    EXPECT_EQ(1, connection->affectedRows());
}

TEST_F(SqlTests, std_string_with_null_and_UTF8_proof_of_concept)
{
    constexpr auto str {"H¥\0l"};  // const char*
    constexpr int len {5};         // ¥ is 2 bytes

    std::string const aa {str, len};  // basic_string(const CharT*, size_type)

    EXPECT_EQ(aa.size(), len);

    EXPECT_EQ(aa[0], 'H');
    EXPECT_EQ(aa[1], '\xC2');  // UTF-8 1st byte
    EXPECT_EQ(aa[2], '\xA5');  // UTF-8 2nd byte
    EXPECT_EQ(aa[3], '\0');    // included null
    EXPECT_EQ(aa[4], 'l');
    EXPECT_EQ(aa[5], '\0');  // trailing null
}

TEST_F(SqlTests, blob_from_char_string_not_containing_nulls)
{
    auto const aa {"H¥l"};

    connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)")
        .execute("row812", "€son", 8888, 8.8, aa);

    auto const result = connection->prepare("SELECT blob_col FROM Test WHERE int_col = ?")
                            .execute(8888)
                            .fieldT<std::string>()
                            .value();
    EXPECT_EQ(std::string {aa}, result);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '8888'");
}

TEST_F(SqlTests, blob_from_std_string_not_containing_nulls)
{
    std::string const aa {"H¥l"};

    connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)")
        .execute("row812", "€son", 8888, 8.8, aa);

    auto const result = connection->prepare("SELECT blob_col FROM Test WHERE int_col =?")
                            .execute(8888)
                            .fieldT<std::string>();

    EXPECT_EQ(aa, result);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '8888'");
}

TEST_F(SqlTests, blob_from_std_string_containing_nulls)
{
    std::string const aa {"H¥\0l", 5};  // NB: Length is specified

    connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)")
        .execute("row812", "€son", 8888, 8.8, aa);

    auto const result = connection->prepare("SELECT blob_col FROM Test WHERE int_col = ?")
                            .execute(8888)
                            .fieldT<std::string>();

    EXPECT_EQ(aa, result);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '8888'");
}

TEST_F(SqlTests, blob_from_and_to_file)
{
    std::filesystem::path const filePathSrc {"../stuff/Test.jpg"};
    auto const sizeSrc = file_size(filePathSrc);

    std::filesystem::path const filePathDes {"../stuff/TestCopy.jpg"};

    connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)")
        .execute("row812", "€son", 9999, 8.8, filePathSrc);

    int const result = connection->prepare("SELECT blob_col FROM Test WHERE int_col = ?")
                           .execute(9999)
                           .toFile(filePathDes, Resultset::FileReplace::no);

    EXPECT_EQ(sizeSrc, result);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '9999'");
    std::remove(filePathDes.c_str());
}

TEST_F(SqlTests, blob_from_and_to_file_replace)
{
    std::filesystem::path const filePathSrc {"../stuff/Test.jpg"};
    auto const sizeSrc = file_size(filePathSrc);

    std::filesystem::path const filePathDes {"../stuff/TestCopy.jpg"};
    copy_file(filePathSrc, filePathDes);

    connection->prepare("INSERT INTO Test VALUES (?, ?, ?, ?, ?)")
        .execute("row812", "€son", 9999, 8.8, filePathSrc);

    int const result = connection->prepare("SELECT blob_col FROM Test WHERE int_col = ?")
                           .execute(9999)
                           .toFile(filePathDes, Resultset::FileReplace::yes);

    EXPECT_EQ(sizeSrc, result);

    connection->quickQuery("DELETE FROM Test WHERE int_col = '9999'");
    std::remove(filePathDes.c_str());
}

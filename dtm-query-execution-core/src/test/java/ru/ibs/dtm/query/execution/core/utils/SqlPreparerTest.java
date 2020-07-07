package ru.ibs.dtm.query.execution.core.utils;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.ibs.dtm.query.execution.core.utils.SqlPreparer.getTableWithSchema;

class SqlPreparerTest {

  String createIfTable = "create table if not exists obj ()";
  String createTable = "create table obj ()";
  String createTableWithSchema = "create table dtmservice.obj ()";

  @Test
  void getTableWithSchemaMnemonic() {
    assertThat("dtmservice.table", equalTo(getTableWithSchema("dtmservice", "table")));
  }

  @Test
  void getTableWithSchemaMnemonicNull() {
    assertThat("dtmservice.table", equalTo(getTableWithSchema("", "table")));
  }

  @Test
  void getTableWithSchemaMnemonicAndSchema() {
    assertThat("dtmservice.table", equalTo(getTableWithSchema("dtmservice", "schema1.table")));
  }

  @Test
  void getTableWithSchemaMnemonicNullAndSchema() {
    assertThat("schema1.table", equalTo(getTableWithSchema("", "schema1.table")));
  }

  @Test
  void getTableWithSchemaMnemonicNullAndSchemaNull() {
    assertThat("dtmservice.table", equalTo(getTableWithSchema("", "table")));
  }

  @Test
  void replaceTableInSql() {
    String result = SqlPreparer.replaceTableInSql(createTable, "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceTableInSqlWithSchema() {
    String result = SqlPreparer.replaceTableInSql(createTableWithSchema, "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceTableInSqlUpperCase() {
    String result = SqlPreparer.replaceTableInSql(createTable.toUpperCase(), "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceTableInSqlIf() {
    String result = SqlPreparer.replaceTableInSql(createIfTable, "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceTableInSqlIfUpperCase() {
    String result = SqlPreparer.replaceTableInSql(createIfTable.toUpperCase(), "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceTableWithSchemaUnderscore() {
    String result = SqlPreparer.replaceTableInSql("create table if not exists test_datamart.doc (", "dtmservice.obj");
    assertThat(result, containsString(" dtmservice.obj "));
  }

  @Test
  void replaceQuoting() {
    String input = "create table tbl(\"index\" varchar(50))";
    String expectedResult = "create table tbl(`index` varchar(50))";
    assertEquals(expectedResult, SqlPreparer.replaceQuote(input));
  }

  @Test
  void removeDistributeBy() {
    String result = SqlPreparer.removeDistributeBy("create table shares.accounts\r\n" +
            "(\n" +
            "    account_id bigint,\r\n" +
            "    account_type varchar(1),\r\n" +
            "    primary key (account_id)\r\n" +
            ") distributed by (account_id)");
    System.out.println(result);
  }

}

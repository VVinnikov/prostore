package io.arenadata.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlCreateDatabase extends SqlCreate {

  private final SqlIdentifier name;

  private static final SqlOperator OPERATOR_DATABASE =
    new SqlSpecialOperator("CREATE DATABASE", SqlKind.CREATE_SCHEMA);


  public SqlCreateDatabase(SqlParserPos pos, boolean ifNotExists, SqlIdentifier name) {
    super(OPERATOR_DATABASE, pos, false, ifNotExists);
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getName() {
    return name;
  }

  public boolean ifNotExists() {
    return this.ifNotExists;
  }
}
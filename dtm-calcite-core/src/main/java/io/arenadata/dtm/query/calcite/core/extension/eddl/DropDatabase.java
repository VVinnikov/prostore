package io.arenadata.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class DropDatabase extends SqlDrop {
  private final SqlIdentifier name;

  private static final SqlOperator OPERATOR_DATABASE =
    new SqlSpecialOperator("DROP DATABASE", SqlKind.DROP_SCHEMA);

  public DropDatabase(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR_DATABASE, pos, ifExists);
    this.name = name;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getName() {
    return name;
  }
}
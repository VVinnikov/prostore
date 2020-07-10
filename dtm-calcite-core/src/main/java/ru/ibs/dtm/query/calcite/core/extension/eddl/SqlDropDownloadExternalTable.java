package ru.ibs.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlDropDownloadExternalTable extends SqlDrop {
  private static final SqlOperator OPERATOR =
    new SqlSpecialOperator("DROP DOWNLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

  private final SqlIdentifier name;

  public SqlDropDownloadExternalTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR, pos, ifExists);
    this.name = name;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(this.name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (this.ifExists) {
      writer.keyword("IF EXISTS");
    }

    this.name.unparse(writer, leftPrec, rightPrec);
  }
}

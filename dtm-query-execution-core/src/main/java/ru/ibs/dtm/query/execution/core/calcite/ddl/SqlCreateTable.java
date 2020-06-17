package ru.ibs.dtm.query.execution.core.calcite.ddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.ibs.dtm.query.execution.core.calcite.eddl.ChunkSizeOperator;
import ru.ibs.dtm.query.execution.core.calcite.eddl.FormatOperator;
import ru.ibs.dtm.query.execution.core.calcite.eddl.LocationOperator;

import java.util.List;
import java.util.Objects;

public class SqlCreateTable extends SqlCreate {
  private final SqlIdentifier name;
  private final LocationOperator locationOperator;
  private final FormatOperator formatOperator;
  private final ChunkSizeOperator chunkSizeOperator;

  private static final SqlOperator OPERATOR_TABLE =
    new SqlSpecialOperator("CREATE DOWNLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

  public SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
                        SqlIdentifier name, SqlNodeList columnList, SqlNode query, SqlNode distributedBy) {
    super(OPERATOR_TABLE, pos, false, ifNotExists);
    this.name = Objects.requireNonNull(name);
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, locationOperator, formatOperator, chunkSizeOperator);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    locationOperator.unparse(writer, leftPrec, rightPrec);
    formatOperator.unparse(writer, leftPrec, rightPrec);
    chunkSizeOperator.unparse(writer, leftPrec, rightPrec);
  }
}

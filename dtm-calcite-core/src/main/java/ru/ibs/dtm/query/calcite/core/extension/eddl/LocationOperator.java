package ru.ibs.dtm.query.calcite.core.extension.eddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import ru.ibs.dtm.common.plugin.exload.Type;

import java.util.List;

public class LocationOperator extends SqlCall {

  private final Type type;
  private final String location;

  private static final SqlOperator OPERATOR_LOCATION =
    new SqlSpecialOperator("LOCATION", SqlKind.OTHER_DDL);

  private static final String DELIMITER = ":";

  LocationOperator(SqlParserPos pos, SqlCharStringLiteral destinationInfo) {
    super(pos);

    String location = destinationInfo.getNlsString().getValue();
    String[] strings = location.split(DELIMITER);
    if (strings.length < 2) {
      throw new IllegalArgumentException("Не задан тип данных в строке " + destinationInfo);
    }

    this.type = Type.findByName(strings[0]);
    this.location = location;

  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR_LOCATION;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(null);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    writer.keyword(this.type + DELIMITER + this.location);
  }

  public Type getType() {
    return type;
  }

  public String getLocation() {
    return location;
  }
}
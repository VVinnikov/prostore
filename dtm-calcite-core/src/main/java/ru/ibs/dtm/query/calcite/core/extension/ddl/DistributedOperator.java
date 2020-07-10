package ru.ibs.dtm.query.calcite.core.extension.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class DistributedOperator extends SqlCall {

	private final SqlNodeList distributedBy;

	private static final SqlOperator DISTRIBUTED_OP =
			new SqlSpecialOperator("DISTRIBUTED BY", SqlKind.OTHER_DDL);

	public DistributedOperator(SqlParserPos pos, SqlNodeList distributedBy) {
		super(pos);
		this.distributedBy = distributedBy;
	}

	@Override
	public SqlOperator getOperator() {
		return DISTRIBUTED_OP;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(null);
	}

	public SqlNodeList getDistributedBy() {
		return distributedBy;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(this.getOperator().getName());
		if (distributedBy != null) {
			SqlWriter.Frame frame = writer.startList("(", ")");
			for (SqlNode c : distributedBy) {
				writer.sep(",");
				c.unparse(writer, 0, 0);
			}
			writer.endList(frame);
		}
	}
}

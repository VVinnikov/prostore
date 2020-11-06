package io.arenadata.dtm.query.calcite.core.extension.ddl;

import io.arenadata.dtm.common.reader.SourceType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlCreateTable extends SqlCreate {
	private final SqlIdentifier name;
	private final SqlNodeList columnList;
	private final SqlNode query;
	private final DistributedOperator distributedBy;
	private final Set<SourceType> destination;

	private static final SqlOperator OPERATOR =
			new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

	public SqlCreateTable(SqlParserPos pos,
						  boolean replace,
						  boolean ifNotExists,
						  SqlIdentifier name,
						  SqlNodeList columnList,
						  SqlNode query,
						  SqlNodeList distributedBy,
						  SqlNodeList destination) {
		super(OPERATOR, pos, false, ifNotExists);
		this.name = name;
		this.columnList = columnList;
		this.query = query;
		this.distributedBy = new DistributedOperator(pos, distributedBy);
		this.destination = Optional.ofNullable(destination)
				.map(nodeList -> nodeList.getList().stream()
					.map(node -> SourceType.valueOfAvailable(node.toString()))
					.collect(Collectors.toSet()))
				.orElse(null);
	}

	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name, columnList, query, distributedBy);
	}

	public DistributedOperator getDistributedBy() {
		return distributedBy;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		writer.keyword("TABLE");
		if (ifNotExists) {
			writer.keyword("IF NOT EXISTS");
		}
		name.unparse(writer, leftPrec, rightPrec);
		if (columnList != null) {
			SqlWriter.Frame frame = writer.startList("(", ")");
			for (SqlNode c : columnList) {
				writer.sep(",");
				c.unparse(writer, 0, 0);
			}
			writer.endList(frame);
		}
		if (distributedBy != null) {
			distributedBy.unparse(writer, 0, 0);
		}
		if (query != null) {
			writer.keyword("AS");
			writer.newlineAndIndent();
			query.unparse(writer, 0, 0);
		}
	}

	public Set<SourceType> getDestination() {
		return destination;
	}
}

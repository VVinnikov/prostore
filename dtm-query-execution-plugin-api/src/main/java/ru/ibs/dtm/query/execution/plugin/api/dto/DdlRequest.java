package ru.ibs.dtm.query.execution.plugin.api.dto;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType;

/**
 * dto для выполнения ddl
 */
public class DdlRequest extends BaseRequest {

	/**
	 * Модель таблицы в служебной БД
	 */
	private final ClassTable classTable;
	private final DdlQueryType queryType;

	public DdlRequest(final QueryRequest queryRequest, final DdlQueryType queryType) {
		this(queryRequest, null, queryType);
	}

	public DdlRequest(final QueryRequest queryRequest, final ClassTable classTable, final DdlQueryType queryType) {
		super(queryRequest);
		this.classTable = classTable;
		this.queryType = queryType;
	}

	public ClassTable getClassTable() {
		return classTable;
	}

	public DdlQueryType getQueryType() {
		return queryType;
	}

	@Override
	public String toString() {
		return "DdlRequest{" +
				super.toString() +
				", classTable=" + classTable +
				", queryType=" + queryType +
				'}';
	}
}

package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;


public class EdmlRequest extends DatamartRequest {

	private final ClassTable classTable;

	public EdmlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public EdmlRequest(final QueryRequest queryRequest, final ClassTable classTable) {
		super(queryRequest);
		this.classTable = classTable;
	}

	public ClassTable getClassTable() {
		return classTable;
	}

	@Override
	public String toString() {
		return "EdmlRequest{" +
				super.toString() +
				", classTable=" + classTable +
				'}';
	}
}

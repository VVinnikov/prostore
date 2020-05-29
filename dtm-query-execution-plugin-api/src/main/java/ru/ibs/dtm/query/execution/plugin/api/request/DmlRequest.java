package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;


public class DmlRequest extends DatamartRequest {

	private final ClassTable classTable;

	public DmlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public DmlRequest(final QueryRequest queryRequest, final ClassTable classTable) {
		super(queryRequest);
		this.classTable = classTable;
	}

	public ClassTable getClassTable() {
		return classTable;
	}

	@Override
	public String toString() {
		return "DmlRequest{" +
				super.toString() +
				", classTable=" + classTable +
				'}';
	}
}

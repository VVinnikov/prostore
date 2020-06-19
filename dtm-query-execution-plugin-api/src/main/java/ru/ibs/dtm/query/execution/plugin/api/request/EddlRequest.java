package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;


public class EddlRequest extends DatamartRequest {

	private final ClassTable classTable;

	public EddlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public EddlRequest(final QueryRequest queryRequest, final ClassTable classTable) {
		super(queryRequest);
		this.classTable = classTable;
	}

	public ClassTable getClassTable() {
		return classTable;
	}

	@Override
	public String toString() {
		return "EddlRequest{" +
				super.toString() +
				", classTable=" + classTable +
				'}';
	}
}

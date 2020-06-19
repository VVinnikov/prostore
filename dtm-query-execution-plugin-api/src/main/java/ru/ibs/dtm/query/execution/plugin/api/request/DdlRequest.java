package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.DDL;


public class DdlRequest extends DatamartRequest {

	private ClassTable classTable;

	public DdlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public DdlRequest(final QueryRequest queryRequest, final ClassTable classTable) {
		super(queryRequest);
		this.classTable = classTable;
	}

	public ClassTable getClassTable() {
		return classTable;
	}

	public void setClassTable(ClassTable classTable) {
		this.classTable = classTable;
	}

	@Override
	public String toString() {
		return "DdlRequest{" +
				super.toString() +
				", classTable=" + classTable +
				'}';
	}
}

package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.reader.QueryRequest;


public class EdmlRequest extends DatamartRequest {

	private final Entity entity;

	public EdmlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public EdmlRequest(final QueryRequest queryRequest, final Entity entity) {
		super(queryRequest);
		this.entity = entity;
	}

	public Entity getClassTable() {
		return entity;
	}

	@Override
	public String toString() {
		return "EdmlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}

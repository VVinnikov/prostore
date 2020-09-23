package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.reader.QueryRequest;


public class DdlRequest extends DatamartRequest {

	private Entity entity;

	public DdlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public DdlRequest(final QueryRequest queryRequest, final Entity entity) {
		super(queryRequest);
		this.entity = entity;
	}

	public Entity getClassTable() {
		return entity;
	}

	public void setClassTable(Entity entity) {
		this.entity = entity;
	}

	@Override
	public String toString() {
		return "DdlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}

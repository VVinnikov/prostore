package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;


public class DmlRequest extends DatamartRequest {

	private final Entity entity;

	public DmlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public DmlRequest(final QueryRequest queryRequest, final Entity entity) {
		super(queryRequest);
		this.entity = entity;
	}

	public Entity getClassTable() {
		return entity;
	}

	@Override
	public String toString() {
		return "DmlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}
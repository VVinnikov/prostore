package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;


public class DdlRequest extends DatamartRequest {

	private Entity entity;

	public DdlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public DdlRequest(final QueryRequest queryRequest, final Entity entity) {
		super(queryRequest);
		this.entity = entity;
	}

	public Entity getEntity() {
		return entity;
	}

	public void setEntity(Entity entity) {
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
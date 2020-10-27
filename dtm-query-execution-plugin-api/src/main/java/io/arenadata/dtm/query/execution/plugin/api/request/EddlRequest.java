package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;


public class EddlRequest extends DatamartRequest {

	private final Entity entity;

	public EddlRequest(final QueryRequest queryRequest) {
		this(queryRequest, null);
	}

	public EddlRequest(final QueryRequest queryRequest, final Entity entity) {
		super(queryRequest);
		this.entity = entity;
	}

	public Entity getClassTable() {
		return entity;
	}

	@Override
	public String toString() {
		return "EddlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}
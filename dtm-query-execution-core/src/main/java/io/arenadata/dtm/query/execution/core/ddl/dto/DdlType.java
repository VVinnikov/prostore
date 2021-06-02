package io.arenadata.dtm.query.execution.core.ddl.dto;

import lombok.ToString;

@ToString
public enum DdlType {
	UNKNOWN(false),
	CREATE_SCHEMA(false),
	DROP_SCHEMA(false),
	CREATE_TABLE(true),
	DROP_TABLE(false),
	CREATE_VIEW(false),
	DROP_VIEW(false),
	CREATE_MATERIALIZED_VIEW(true),
	DROP_MATERIALIZED_VIEW(false),
	;

	boolean createTopic;

	DdlType(boolean createTopic) {
		this.createTopic = createTopic;
	}

	public boolean isCreateTopic() {
		return createTopic;
	}

}

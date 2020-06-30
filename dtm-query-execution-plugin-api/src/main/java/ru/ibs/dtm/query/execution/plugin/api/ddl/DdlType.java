package ru.ibs.dtm.query.execution.plugin.api.ddl;

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
	;

	boolean createTopic;

	DdlType(boolean createTopic) {
		this.createTopic = createTopic;
	}

	public boolean isCreateTopic() {
		return createTopic;
	}

}

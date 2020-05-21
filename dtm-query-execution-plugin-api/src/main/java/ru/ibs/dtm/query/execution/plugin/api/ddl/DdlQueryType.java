package ru.ibs.dtm.query.execution.plugin.api.ddl;

import lombok.ToString;

@ToString
public enum DdlQueryType {

	CREATE_SCHEMA(false),
	DROP_SCHEMA(false),
	CREATE_TABLE(true),
	DROP_TABLE(false),
	;

	boolean createTopic;

	DdlQueryType(boolean createTopic) {
		this.createTopic = createTopic;
	}

	public boolean isCreateTopic() {
		return createTopic;
	}

}

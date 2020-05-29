package ru.ibs.dtm.query.execution.plugin.api.request;

import ru.ibs.dtm.common.reader.QueryRequest;

public class DatamartRequest {

	private QueryRequest queryRequest;

	public DatamartRequest(QueryRequest queryRequest) {
		this.queryRequest = queryRequest;
	}

	public QueryRequest getQueryRequest() {
		return queryRequest;
	}

	public void setQueryRequest(QueryRequest queryRequest) {
		this.queryRequest = queryRequest;
	}

	@Override
	public String toString() {
		return "DatamartRequest{" +
				"queryRequest=" + getQueryRequest() +
				'}';
	}
}

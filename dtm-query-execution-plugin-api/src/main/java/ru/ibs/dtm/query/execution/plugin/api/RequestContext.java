package ru.ibs.dtm.query.execution.plugin.api;

public class RequestContext<Request> {

	private Request request;

	public RequestContext(Request request) {
		this.request = request;
	}

	public Request getRequest() {
		return request;
	}

}

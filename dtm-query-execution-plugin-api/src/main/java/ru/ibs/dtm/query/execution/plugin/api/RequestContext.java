package ru.ibs.dtm.query.execution.plugin.api;

public class RequestContext<Request, Command> {

	private Request request;
	private Command command;

	public RequestContext(Request request, Command command) {
		this.request = request;
		this.command = command;
	}

	public Request getRequest() {
		return request;
	}

	public Command getCommand() {
		return command;
	}

}

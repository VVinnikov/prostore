package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.reader.QueryRequest;

public class LlrRequest extends DatamartRequest {

	private JsonObject schema;

	public LlrRequest(QueryRequest queryRequest, JsonObject schema) {
		super(queryRequest);
		this.schema = schema;
	}

	public JsonObject getSchema() {
		return schema;
	}

	public void setSchema(JsonObject schema) {
		this.schema = schema;
	}

	@Override
	public String toString() {
		return "LlrRequest{" +
				super.toString() +
				", schema=" + schema +
				'}';
	}
}

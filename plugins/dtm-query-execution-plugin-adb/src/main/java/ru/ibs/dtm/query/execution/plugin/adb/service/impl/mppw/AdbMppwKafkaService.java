package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

@Component("adbMppwKafkaService")
public class AdbMppwKafkaService implements MppwKafkaService<QueryResult> {
	private final WebClient webClient;
	private final MppwProperties mppwProperties;

	public AdbMppwKafkaService(@Qualifier("adbWebClient") WebClient webClient,
							   MppwProperties mppwProperties) {
		this.webClient = webClient;
		this.mppwProperties = mppwProperties;
	}

	@Override
	public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		MppwRequest mppwRequest = context.getRequest();

		RestLoadRequest request = new RestLoadRequest();
		request.setHotDelta(mppwRequest.getQueryLoadParam().getDeltaHot());
		request.setDatamart(mppwRequest.getQueryLoadParam().getDatamart());
		request.setTableName(mppwRequest.getQueryLoadParam().getTableName());
		request.setZookeeperHost(mppwRequest.getZookeeperHost());
		request.setZookeeperPort(mppwRequest.getZookeeperPort());
		request.setKafkaTopic(mppwRequest.getTopic());
		request.setConsumerGroup(mppwProperties.getConsumerGroup());
		request.setFormat(mppwRequest.getQueryLoadParam().getFormat().getName());

		try {
			val schema = new Schema.Parser().parse(mppwRequest.getSchema().encode());
			request.setSchema(schema);
			initiateLoading(request).onComplete(asyncHandler);
		} catch (Exception e) {
			asyncHandler.handle(Future.failedFuture(e));
		}
	}

	private Future<QueryResult> initiateLoading(RestLoadRequest request) {
		try {
			JsonObject data = JsonObject.mapFrom(request);
			Promise<QueryResult> promise = Promise.promise();
			webClient.postAbs(mppwProperties.getLoadUrl()).sendJsonObject(data, ar -> {
				if (ar.succeeded()) {
					HttpResponse<Buffer> response = ar.result();
					if (response.statusCode() < 400 && response.statusCode() >= 200) {
						promise.complete(QueryResult.emptyResult());
					} else {
						promise.fail(new RuntimeException(String.format("Received HTTP status %s, msg %s", response.statusCode(), response.bodyAsString())));
					}
				} else {
					promise.fail(ar.cause());
				}
			});
			return promise.future();
		} catch (Exception e) {
			return Future.failedFuture(e);
		}
	}
}

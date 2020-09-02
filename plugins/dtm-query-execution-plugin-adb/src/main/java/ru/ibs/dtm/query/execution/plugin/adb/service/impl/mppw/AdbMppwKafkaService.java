package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.eventbus.DataTopic;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

@Slf4j
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
		log.debug("mppw start");
		MppwRequest mppwRequest = context.getRequest();

		RestLoadRequest request = new RestLoadRequest();
		request.setRequestId(context.getRequest().getQueryRequest().getRequestId().toString());
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
			initiateLoading(request, mppwRequest.getLoadStart()?
					mppwProperties.getStartLoadUrl(): mppwProperties.getStopLoadUrl()).onComplete(asyncHandler);
		} catch (Exception e) {
			asyncHandler.handle(Future.failedFuture(e));
		}
	}

	private Future<QueryResult> initiateLoading(RestLoadRequest request, String path) {
		try {
			JsonObject data = JsonObject.mapFrom(request);
			Promise<QueryResult> promise = Promise.promise();
			log.debug("Send request to emulator-writer: [{}]", path);
			webClient.postAbs(path).sendJsonObject(data, ar -> {
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

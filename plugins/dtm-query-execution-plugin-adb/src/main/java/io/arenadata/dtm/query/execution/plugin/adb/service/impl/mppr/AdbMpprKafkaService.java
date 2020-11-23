package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService<QueryResult> {
	private static final Logger LOG = LoggerFactory.getLogger(AdbMpprKafkaService.class);

	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final MetadataSqlFactory metadataSqlFactory;
	private final AdbQueryExecutor adbQueryExecutor;

	public AdbMpprKafkaService(QueryEnrichmentService adbQueryEnrichmentService, MetadataSqlFactory metadataSqlFactory, AdbQueryExecutor adbQueryExecutor) {
		this.adbQueryEnrichmentService = adbQueryEnrichmentService;
		this.metadataSqlFactory = metadataSqlFactory;
		this.adbQueryExecutor = adbQueryExecutor;
	}

	@Override
	public void execute(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		MpprRequest request = context.getRequest();
		adbQueryEnrichmentService.enrich(
				EnrichQueryRequest.generate(request.getQueryRequest(), request.getLogicalSchema()),
				sqlResult -> {
					if (sqlResult.succeeded()) {
						val schema = context.getRequest().getQueryRequest().getDatamartMnemonic();
						val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF + context.getRequest().getQueryRequest().getRequestId().toString().replaceAll("-", "_");
						val columns = context.getRequest().getDestinationEntity().getFields().stream()
								.map(field -> field.getName() + " " + EntityTypeUtil.pgFromDtmType(field)).collect(Collectors.toList());
						val topic = context.getRequest().getKafkaParameter().getTopic();
						val brokers = context.getRequest().getKafkaParameter().getBrokers().stream()
								.map(kafkaBrokerInfo -> kafkaBrokerInfo.getAddress()).collect(Collectors.toList());
						val chunkSize = ((DownloadExternalEntityMetadata) context.getRequest().getKafkaParameter().getDownloadMetadata()).getChunkSize();
						createWritableExtTable(schema, table, columns, topic, brokers, chunkSize)
								.compose(v -> executeInsert(schema, table, sqlResult.result()))
								.compose(v -> dropWritableExtTable(schema, table))
								.onSuccess(success -> {
									asyncHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
								})
								.onFailure(err -> {
									dropWritableExtTable(schema, table)
											.onComplete(dropResult -> {
												if (dropResult.failed()) {
													log.error("Failed to drop writable external table {}.{}", schema, table);
												}
												asyncHandler.handle(Future.failedFuture(err));
											});
								});
					} else {
						LOG.error("Error while enriching request");
						asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
					}
				});
	}

	private Future<Void> createWritableExtTable(String schema, String table, List<String> columns, String topic, List<String> brokerList, Integer chunkSize){
		return Future.future(p -> {
			adbQueryExecutor.executeUpdate(metadataSqlFactory.createWritableExtTableSqlQuery(schema, table, columns, topic, brokerList, chunkSize), p);
		});
	}

	private Future<Void> executeInsert(String schema, String table, String enrichedSql) {
		return Future.future(p -> {
			adbQueryExecutor.executeUpdate(metadataSqlFactory.insertIntoWritableExtTableSqlQuery(schema, table, enrichedSql), p);
		});
	}

	private Future<Void> dropWritableExtTable(String schema, String table) {
		return Future.future(p -> {
			adbQueryExecutor.executeUpdate(metadataSqlFactory.dropWritableExtTableSqlQuery(schema, table), p);
		});
	}


}

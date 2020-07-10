package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import ru.ibs.dtm.query.execution.core.utils.DefaultDatamartSetter;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

@Slf4j
@Component
public class QueryAnalyzerImpl implements QueryAnalyzer {

	private final QueryDispatcher queryDispatcher;
	private final DefinitionService<SqlNode> definitionService;
	private final Vertx vertx;
	private final HintExtractor hintExtractor;
	private final RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory;
	private final AppConfiguration configuration;
	private final DatamartMnemonicExtractor datamartMnemonicExtractor;
	private final DefaultDatamartSetter defaultDatamartSetter;

	@Autowired
	public QueryAnalyzerImpl(QueryDispatcher queryDispatcher,
							 @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
							 RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory,
							 @Qualifier("coreVertx") Vertx vertx,
							 HintExtractor hintExtractor,
							 DatamartMnemonicExtractor datamartMnemonicExtractor,
							 AppConfiguration configuration,
							 DefaultDatamartSetter defaultDatamartSetter) {
		this.queryDispatcher = queryDispatcher;
		this.definitionService = definitionService;
		this.requestContextFactory = requestContextFactory;
		this.vertx = vertx;
		this.hintExtractor = hintExtractor;
		this.datamartMnemonicExtractor = datamartMnemonicExtractor;
		this.configuration = configuration;
		this.defaultDatamartSetter = defaultDatamartSetter;
	}

	@Override
	public void analyzeAndExecute(QueryRequest queryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		getParsedQuery(queryRequest, parseResult -> {
			if (parseResult.succeeded()) {
				queryRequest.setSystemName(configuration.getSystemName());
				SqlNode sqlNode = parseResult.result();
				if (queryRequest.getDatamartMnemonic() == null) {
					datamartMnemonicExtractor.extract(sqlNode).ifPresent(queryRequest::setDatamartMnemonic);
				}
				sqlNode = defaultDatamartSetter.set(sqlNode, queryRequest.getDatamartMnemonic());
				queryDispatcher.dispatch(
						requestContextFactory.create(queryRequest, sqlNode), asyncResultHandler
				);
			} else {
				log.debug("Ошибка анализа запроса", parseResult.cause());
				asyncResultHandler.handle(Future.failedFuture(parseResult.cause()));
			}
		});
	}

	private void getParsedQuery(QueryRequest queryRequest,
								Handler<AsyncResult<SqlNode>> asyncResultHandler) {
		vertx.executeBlocking(it ->
			{
				try {
					val hint = hintExtractor.extractHint(queryRequest);
					val query = hint.getQueryRequest().getSql();
					log.debug("Предпарсинг запроса: {}", query);
					val node = definitionService.processingQuery(query);
					it.complete(node);
				} catch (Exception e){
					log.error("Ошибка парсинга запроса", e);
					it.fail(e);
				}
			}
				, ar -> {
			if (ar.succeeded()) {
				asyncResultHandler.handle(Future.succeededFuture((SqlNode) ar.result()));
			} else {
				asyncResultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

}

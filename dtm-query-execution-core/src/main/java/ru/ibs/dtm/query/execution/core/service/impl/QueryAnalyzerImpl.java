package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.factory.RequestContextFactory;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.QueryAnalyzer;
import ru.ibs.dtm.query.execution.core.service.QueryDispatcher;
import ru.ibs.dtm.query.execution.core.utils.DatamartMnemonicExtractor;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

@Slf4j
@Component
public class QueryAnalyzerImpl implements QueryAnalyzer {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryAnalyzerImpl.class);

	private final QueryDispatcher queryDispatcher;
	private final DefinitionService<SqlNode> definitionService;
	private final Vertx vertx;
	private final HintExtractor hintExtractor;
	private final RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory;
	private final DatamartMnemonicExtractor datamartMnemonicExtractor;

	@Autowired
	public QueryAnalyzerImpl(QueryDispatcher queryDispatcher,
							 DefinitionService<SqlNode> definitionService,
							 RequestContextFactory<RequestContext<? extends DatamartRequest>, QueryRequest> requestContextFactory,
							 @Qualifier("coreVertx") Vertx vertx,
							 HintExtractor hintExtractor,
							 DatamartMnemonicExtractor datamartMnemonicExtractor) {
		this.queryDispatcher = queryDispatcher;
		this.definitionService = definitionService;
		this.requestContextFactory = requestContextFactory;
		this.vertx = vertx;
		this.hintExtractor = hintExtractor;
		this.datamartMnemonicExtractor = datamartMnemonicExtractor;
	}

	@Override
	public void analyzeAndExecute(QueryRequest queryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		getParsedQuery(queryRequest, parseResult -> {
			if (parseResult.succeeded()) {
				SqlNode sqlNode = parseResult.result();
				if (queryRequest.getDatamartMnemonic() == null) {
					try {
						datamartMnemonicExtractor.extract(sqlNode).ifPresent(queryRequest::setDatamartMnemonic);
					} catch (Exception ex) {
						log.error("Datamart mnemonic is not extract from sql [{}]: {}", sqlNode, ex);
						asyncResultHandler.handle(Future.failedFuture(ex));
					}
				}
				queryDispatcher.dispatch(
						requestContextFactory.create(queryRequest, sqlNode), asyncResultHandler
				);
			} else {
				LOGGER.debug("Ошибка анализа запроса", parseResult.cause());
				asyncResultHandler.handle(Future.failedFuture(parseResult.cause()));
			}
		});
	}

	private void getParsedQuery(QueryRequest queryRequest,
								Handler<AsyncResult<SqlNode>> asyncResultHandler) {
		vertx.executeBlocking(it ->
				hintExtractor.extractHint(queryRequest, arHint -> {
					if (!arHint.succeeded()) {
						it.fail(arHint.cause());
						return;
					}
					try {
						String query = arHint.result().getQueryRequest().getSql();
						LOGGER.debug("Предпарсинг запроса: {}", query);
						SqlNode node = definitionService.processingQuery(query);
						it.complete(node);
					} catch (Exception e) {
						LOGGER.error("Ошибка парсинга запроса", e);
						it.fail(e);
					}
				}), ar -> {
			if (ar.succeeded()) {
				asyncResultHandler.handle(Future.succeededFuture((SqlNode) ar.result()));
			} else {
				asyncResultHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

}

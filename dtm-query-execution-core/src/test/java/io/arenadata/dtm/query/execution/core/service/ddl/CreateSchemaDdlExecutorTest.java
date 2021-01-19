package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.DatamartDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.service.ddl.impl.CreateSchemaDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.impl.MetadataExecutorImpl;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
//FixMe Test
class CreateSchemaDdlExecutorTest {


//    private final MetadataExecutor<DdlRequestContext> metadataExecutor = mock(MetadataExecutorImpl.class);
//    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
//    private final ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
//    private final DatamartDao datamartDao = mock(DatamartDaoImpl.class);
//    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
//    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
//    private final SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
//    private QueryResultDdlExecutor createSchemaDdlExecutor;
//    private DdlRequestContext context;
//    private String schema;
//
//    @BeforeEach
//    void setUp() throws SqlParseException {
//        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
//        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
//        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
//        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
//        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
//        createSchemaDdlExecutor = new CreateSchemaDdlExecutor(metadataExecutor, serviceDbFacade);
//        schema = "shares";
//        final QueryRequest queryRequest = new QueryRequest();
//        queryRequest.setRequestId(UUID.randomUUID());
//        queryRequest.setDatamartMnemonic(schema);
//        queryRequest.setSql("create database shares");
//        SqlNode sqlNode = planner.parse(queryRequest.getSql());
//        context = new DdlRequestContext(null, new DdlRequest(queryRequest), sqlNode, null, null);
//    }
//
//    @Test
//    void executeSuccess() {
//        Promise<QueryResult> promise = Promise.promise();
//        Mockito.when(datamartDao.existsDatamart(eq(schema)))
//                .thenReturn(Future.succeededFuture(false));
//
//        when(metadataExecutor.execute(any()))
//                .thenReturn(Future.succeededFuture());
//
//        Mockito.when(datamartDao.createDatamart(eq(schema)))
//                .thenReturn(Future.succeededFuture());
//
//        createSchemaDdlExecutor.execute(context, null)
//                .onComplete(promise);
//
//        assertNotNull(promise.future().result());
//    }
//
//    @Test
//    void executeWithExistDatamart() {
//        Promise<QueryResult> promise = Promise.promise();
//        Mockito.when(datamartDao.existsDatamart(eq(schema)))
//                .thenReturn(Future.succeededFuture(true));
//        createSchemaDdlExecutor.execute(context, null)
//                .onComplete(promise);
//
//        assertTrue(promise.future().failed());
//    }
//
//    @Test
//    void executeWithCheckExistsDatamartError() {
//        Promise<QueryResult> promise = Promise.promise();
//        Mockito.when(datamartDao.existsDatamart(eq(schema)))
//                .thenReturn(Future.failedFuture(new DtmException("exists error")));
//        createSchemaDdlExecutor.execute(context, null)
//                .onComplete(promise);
//
//        assertTrue(promise.future().failed());
//    }
//
//    @Test
//    void executeWithMetadataExecError() {
//        Promise<QueryResult> promise = Promise.promise();
//        Mockito.when(datamartDao.existsDatamart(eq(schema)))
//                .thenReturn(Future.succeededFuture(false));
//
//        when(metadataExecutor.execute(any()))
//                .thenReturn(Future.failedFuture(new DtmException("")));
//
//        createSchemaDdlExecutor.execute(context, null)
//                .onComplete(promise);
//
//        assertTrue(promise.future().failed());
//    }
//
//    @Test
//    void executeWithInsertDatamartError() {
//        Promise<QueryResult> promise = Promise.promise();
//        Mockito.when(datamartDao.existsDatamart(eq(schema)))
//                .thenReturn(Future.succeededFuture(false));
//
//        when(metadataExecutor.execute(any()))
//                .thenReturn(Future.succeededFuture());
//
//        Mockito.when(datamartDao.createDatamart(eq(schema)))
//                .thenReturn(Future.failedFuture(new DtmException("create error")));
//
//        createSchemaDdlExecutor.execute(context, null)
//                .onComplete(promise);
//
//        assertNotNull(promise.future().cause());
//    }
}

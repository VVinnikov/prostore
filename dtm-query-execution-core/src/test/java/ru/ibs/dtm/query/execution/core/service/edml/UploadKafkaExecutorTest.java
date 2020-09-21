package ru.ibs.dtm.query.execution.core.service.edml;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.ibs.dtm.common.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.MppwKafkaRequestFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.impl.UploadKafkaExecutor;
import ru.ibs.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class UploadKafkaExecutorTest {

    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final MppwKafkaRequestFactory mppwKafkaRequestFactory = mock(MppwKafkaRequestFactoryImpl.class);
    private final EdmlProperties edmlProperties = mock(EdmlProperties.class);
    private final KafkaProperties kafkaProperties = mock(KafkaProperties.class);
    private EdmlUploadExecutor uploadKafkaExecutor;
    private Vertx vertx = Vertx.vertx();
    private Set<SourceType> sourceTypes;
    private QueryRequest queryRequest;
    private QueryResult queryResult;
    private Object resultException;
    private Integer inpuStreamTimeoutMs = 2000;
    private Integer pluginStatusCheckPeriodMs = 1000;
    private Integer firstOffsetTimeoutMs = 15000;
    private Integer changeOffsetTimeoutMs = 10000;

    @BeforeEach
    void setUp() {
        uploadKafkaExecutor = new UploadKafkaExecutor(pluginService, mppwKafkaRequestFactory,
                edmlProperties, kafkaProperties, vertx);
        sourceTypes = new HashSet<>();
        sourceTypes.addAll(Arrays.asList(SourceType.ADB, SourceType.ADG));
        queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        queryRequest.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        queryRequest.setSubRequestId("6efad624-b9da-4ba1-9fed-f2da478b08e8");
        queryRequest.setSql("INSERT INTO test.pso SELECT id, name FROM test.upload_table");
    }

    @Test
    void executeMppwAllSuccess() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwAllSuccess", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);
            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);
            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 10, 5);
            initStatusResultQueue(adgStatusResultQueue, 10, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });
            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                handler.handle(Future.succeededFuture());
                return null;
            }).when(pluginService).mppwKafka(eq(SourceType.ADB), eq(mppwAdbContext), any());
            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                handler.handle(Future.succeededFuture());
                return null;
            }).when(pluginService).mppwKafka(eq(SourceType.ADG), eq(mppwAdgContext), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(adbStatusResultQueue.poll()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(adgStatusResultQueue.poll()));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                    async.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(queryResult);
    }

    @Test
    void executeMppwWithAdbPluginStartFail() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithStartFail", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);
            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adgRequest.setLoadStart(true);
            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adgStatusResultQueue, 10, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(adgStatusResultQueue.poll()));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                final MppwRequestContext requestContext = invocation.getArgument(1);
                if (ds.equals(SourceType.ADB) && requestContext.getRequest().getLoadStart()) {
                    handler.handle(Future.failedFuture(new RuntimeException("Ошибка старта mppw")));
                } else if (ds.equals(SourceType.ADB) && !requestContext.getRequest().getLoadStart()) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    resultException = ar.cause();
                    promise.fail(ar.cause());
                }
                async.complete();
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);
    }

    @Test
    void executeMppwWithFailedRetrievePluginStatus() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        RuntimeException exception = new RuntimeException("Ошибка получения статуса");
        suite.test("executeMppwWithFailedRetrievePluginStatus", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);
            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adgRequest.setLoadStart(true);
            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 10, 5);
            initStatusResultQueue(adgStatusResultQueue, 10, 5);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.failedFuture(exception));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.failedFuture(exception));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                final MppwRequestContext requestContext = invocation.getArgument(1);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    resultException = ar.cause();
                    promise.fail(ar.cause());
                }
                async.complete();
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertEquals(resultException, exception);
    }

    @Test
    void executeMppwWithLastOffsetNotIncrease() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithLastOffsetNotIncrease", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);

            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adgRequest.setLoadStart(true);

            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueueWithOffset(adgStatusResultQueue, 15, 5, 1);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(adbStatusResultQueue.poll()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(adgStatusResultQueue.poll()));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                final MppwRequestContext requestContext = invocation.getArgument(1);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    resultException = ar.cause();
                    promise.fail(ar.cause());
                }
                async.complete();
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);
    }

    @Test
    void executeMppwLoadingInitFalure() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwLoadingInitFalure", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);

            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adgRequest.setLoadStart(true);

            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 5);
            initStatusResultQueueWithOffset(adgStatusResultQueue, 15, 5, 0);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(adbStatusResultQueue.poll()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(adgStatusResultQueue.poll()));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    resultException = ar.cause();
                    promise.fail(ar.cause());
                }
                async.complete();
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultException);
    }

    @Test
    void executeMppwWithZeroOffsets() {
        TestSuite suite = TestSuite.create("mppwLoadTest");
        suite.test("executeMppwWithZeroOffsets", context -> {
            Async async = context.async();
            JsonObject schema = new JsonObject();
            Promise promise = Promise.promise();
            KafkaAdminProperty kafkaAdminProperty = new KafkaAdminProperty();
            kafkaAdminProperty.setInputStreamTimeoutMs(inpuStreamTimeoutMs);

            final QueryLoadParam queryLoadParam = createQueryLoadParam();

            LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(queryLoadParam.getLocationPath());
            DatamartRequest request = new DatamartRequest(queryRequest);
            EdmlRequestContext edmlRequestContext = new EdmlRequestContext(request, null);
            edmlRequestContext.setTargetTable(new TableInfo("test", "pso"));
            edmlRequestContext.setSourceTable(new TableInfo("test", "upload_table"));

            final MppwRequest adbRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adbRequest.setTopic(kafkaTopicUri.getTopic());
            adbRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adbRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adbRequest.setLoadStart(true);

            final MppwRequest adgRequest = new MppwRequest(queryRequest, queryLoadParam, schema);
            adgRequest.setTopic(kafkaTopicUri.getTopic());
            adgRequest.setZookeeperHost(kafkaTopicUri.getHost());
            adgRequest.setZookeeperPort(kafkaTopicUri.getPort());
            adgRequest.setLoadStart(true);

            final Queue<MppwRequestContext> mppwContextQueue = new BlockingArrayQueue<>();
            final MppwRequestContext mppwAdbContext = new MppwRequestContext(adbRequest);
            final MppwRequestContext mppwAdgContext = new MppwRequestContext(adgRequest);
            mppwContextQueue.add(mppwAdbContext);
            mppwContextQueue.add(mppwAdgContext);

            final Queue<StatusQueryResult> adbStatusResultQueue = new BlockingArrayQueue<>();
            final Queue<StatusQueryResult> adgStatusResultQueue = new BlockingArrayQueue<>();
            initStatusResultQueue(adbStatusResultQueue, 15, 0);
            initStatusResultQueue(adgStatusResultQueue, 15, 0);

            when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
            when(edmlProperties.getPluginStatusCheckPeriodMs()).thenReturn(pluginStatusCheckPeriodMs);
            when(edmlProperties.getFirstOffsetTimeoutMs()).thenReturn(firstOffsetTimeoutMs);
            when(edmlProperties.getChangeOffsetTimeoutMs()).thenReturn(changeOffsetTimeoutMs);
            when(kafkaProperties.getAdmin()).thenReturn(kafkaAdminProperty);

            when(mppwKafkaRequestFactory.create(edmlRequestContext)).thenAnswer(
                    new Answer<MppwRequestContext>() {
                        @Override
                        public MppwRequestContext answer(InvocationOnMock invocation) {
                            return mppwContextQueue.poll();
                        }
                    });

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<StatusQueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(adbStatusResultQueue.poll()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(adgStatusResultQueue.poll()));
                }
                return null;
            }).when(pluginService).status(any(), any(), any());

            Mockito.doAnswer(invocation -> {
                final Handler<AsyncResult<QueryResult>> handler = invocation.getArgument(2);
                final SourceType ds = invocation.getArgument(0);
                if (ds.equals(SourceType.ADB)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                } else if (ds.equals(SourceType.ADG)) {
                    handler.handle(Future.succeededFuture(new QueryResult()));
                }
                return null;
            }).when(pluginService).mppwKafka(any(), any(), any());

            uploadKafkaExecutor.execute(edmlRequestContext, ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    resultException = ar.cause();
                    promise.fail(ar.cause());
                }
                async.complete();
            });
            async.awaitSuccess();
            queryResult = (QueryResult) promise.future().result();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(resultException);
    }

    private void initStatusResultQueue(Queue<StatusQueryResult> adbStatusResultQueue,
                                       long statusResultCount, long endOffset) {
        long msgProcessTimeoutMs = 1000L;
        final LocalDateTime lastCommitTime = LocalDateTime.now();
        LongStream.range(0L, statusResultCount).forEach(key -> {
            adbStatusResultQueue.add(createStatusQueryResult(lastCommitTime.plus(msgProcessTimeoutMs * key,
                    ChronoField.MILLI_OF_DAY.getBaseUnit()), endOffset, key));
        });
    }

    private void initStatusResultQueueWithOffset(Queue<StatusQueryResult> adbStatusResultQueue,
                                                 long statusResultCount, long endOffset, long offset) {
        long msgProcessTimeoutMs = 1000L;
        final LocalDateTime lastCommitTime = LocalDateTime.now();
        LongStream.range(0L, statusResultCount).forEach(key -> {
            adbStatusResultQueue.add(createStatusQueryResult(lastCommitTime.plus(msgProcessTimeoutMs * key,
                    ChronoField.MILLI_OF_DAY.getBaseUnit()), endOffset, offset));
        });
    }

    private StatusQueryResult createStatusQueryResult(LocalDateTime lastCommitTime, long endOffset, long offset) {
        StatusQueryResult statusQueryResult = new StatusQueryResult();
        KafkaPartitionInfo kafkaPartitionInfo = createKafkaPartitionInfo(lastCommitTime, endOffset, offset);
        statusQueryResult.setPartitionInfo(kafkaPartitionInfo);
        return statusQueryResult;
    }

    @NotNull
    private QueryLoadParam createQueryLoadParam() {
        final QueryLoadParam queryLoadParam = new QueryLoadParam();
        queryLoadParam.setId(UUID.randomUUID());
        queryLoadParam.setDatamart("test");
        queryLoadParam.setDeltaHot(1L);
        queryLoadParam.setFormat(Format.AVRO);
        queryLoadParam.setLocationType(Type.KAFKA_TOPIC);
        queryLoadParam.setMessageLimit(1000);
        queryLoadParam.setLocationPath("kafka://kafka-1.dtm.local:9092/topic");
        queryLoadParam.setSqlQuery(queryRequest.getSql());
        queryLoadParam.setKafkaStreamTimeoutMs(10000);
        queryLoadParam.setPluginStatusCheckPeriodMs(1000);
        return queryLoadParam;
    }

    @NotNull
    private KafkaPartitionInfo createKafkaPartitionInfo(LocalDateTime lastCommitTime, long endOffset, long offset) {
        return KafkaPartitionInfo.builder()
                .topic("topic")
                .start(0L)
                .end(endOffset)
                .lag(0L)
                .offset(offset)
                .endMessageTime(lastCommitTime)
                .lastCommitTime(lastCommitTime)
                .partition(1)
                .build();
    }
}
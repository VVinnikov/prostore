package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckVersions;
import io.arenadata.dtm.query.execution.core.configuration.plugin.properties.ActivePluginsProperties;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.core.factory.CheckVersionQueryResultFactory;
import io.arenadata.dtm.query.execution.core.factory.impl.check.CheckVersionQueryResultFactoryImpl;
import io.arenadata.dtm.query.execution.core.service.check.impl.CheckVersionsExecutor;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.multipart.MultipartForm;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.arenadata.dtm.query.execution.core.factory.impl.check.CheckVersionQueryResultFactoryImpl.COMPONENT_NAME_COLUMN;
import static io.arenadata.dtm.query.execution.core.factory.impl.check.CheckVersionQueryResultFactoryImpl.VERSION_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckVersionsExecutorTest {
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final CheckVersionQueryResultFactory queryResultFactory = mock(CheckVersionQueryResultFactoryImpl.class);
    private final WebClient webClient = mock(WebClient.class);
    private final ActivePluginsProperties activePluginsProperties = new ActivePluginsProperties();
    private final KafkaProperties kafkaProperties = new KafkaProperties();
    private CheckVersionsExecutor versionsExecutor;
    private CheckContext context;

    @BeforeEach
    void setUp() {
        activePluginsProperties.setActivePlugins(SOURCE_TYPES);
        kafkaProperties.setStatusMonitorUrl("http://status-monitor:9095/version");
        versionsExecutor = new CheckVersionsExecutor(dataSourcePluginService,
                queryResultFactory,
                webClient,
                activePluginsProperties,
                kafkaProperties);
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        SqlCheckVersions sqlCheckVersions = mock(SqlCheckVersions.class);
        context = new CheckContext(new RequestMetrics(), "test",
                new DatamartRequest(queryRequest), CheckType.VERSIONS, sqlCheckVersions);
    }

    @Test
    void executeSuccess() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));
        List<VersionInfo> adgVersions = Arrays.asList(new VersionInfo("adg-connector", "3.4.0"));

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        adgVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.succeededFuture(adgVersions);
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(expectedResult, ar.result());
                });
    }

    @Test
    void executeWithNullVersionInfo() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));
        List<VersionInfo> adgVersions = null;

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.succeededFuture(adgVersions);
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(expectedResult, ar.result());
                });
    }

    @Test
    void executeFail() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.failedFuture(new DtmException(""));
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> assertTrue(ar.failed()));
    }

    private Map<String, Object> createRowMap(VersionInfo versionInfo) {
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(COMPONENT_NAME_COLUMN, versionInfo.getName());
        rowMap.put(VERSION_COLUMN, versionInfo.getVersion());
        return rowMap;
    }

    @NotNull
    private HttpRequest<Buffer> createHttpRequest() {
        return new HttpRequest<Buffer>() {
            @Override
            public HttpRequest<Buffer> method(HttpMethod httpMethod) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> rawMethod(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> port(int i) {
                return null;
            }

            @Override
            public <U> HttpRequest<U> as(BodyCodec<U> bodyCodec) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> host(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> virtualHost(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> uri(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeaders(MultiMap multiMap) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeader(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeader(String s, Iterable<String> iterable) {
                return null;
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> basicAuthentication(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> basicAuthentication(Buffer buffer, Buffer buffer1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> bearerTokenAuthentication(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> ssl(Boolean aBoolean) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> timeout(long l) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> addQueryParam(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> setQueryParam(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> followRedirects(boolean b) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> expect(ResponsePredicate responsePredicate) {
                return null;
            }

            @Override
            public MultiMap queryParams() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> copy() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> multipartMixed(boolean b) {
                return null;
            }

            @Override
            public void sendStream(ReadStream<Buffer> readStream, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void sendBuffer(Buffer buffer, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void sendJsonObject(JsonObject jsonObject, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void sendJson(@Nullable Object o, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void sendForm(MultiMap multiMap, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void sendMultipartForm(MultipartForm multipartForm, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public void send(Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
                handler.handle(Future.succeededFuture(createHttpResponse()));
            }
        };
    }

    @NotNull
    private HttpResponse<Buffer> createHttpResponse() {
        return new HttpResponse<Buffer>() {
            @Override
            public HttpVersion version() {
                return null;
            }

            @Override
            public int statusCode() {
                return 200;
            }

            @Override
            public String statusMessage() {
                return null;
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public @Nullable String getHeader(String s) {
                return null;
            }

            @Override
            public MultiMap trailers() {
                return null;
            }

            @Override
            public @Nullable String getTrailer(String s) {
                return null;
            }

            @Override
            public List<String> cookies() {
                return null;
            }

            @Override
            public @Nullable Buffer body() {
                return null;
            }

            @Override
            public @Nullable Buffer bodyAsBuffer() {
                return null;
            }

            @Override
            public List<String> followedRedirects() {
                return null;
            }

            @Override
            public @Nullable JsonArray bodyAsJsonArray() {
                return null;
            }

            @Override
            public <R> @Nullable R bodyAsJson(Class<R> type) {
                return (R) createStatusMonitorVersionInfo();
            }
        };
    }

    @NotNull
    private VersionInfo createStatusMonitorVersionInfo() {
        return new VersionInfo("status-monitor", "3.4.0");
    }
}


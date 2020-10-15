package ru.ibs.dtm.query.calcite.core.delta.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaInterval;
import ru.ibs.dtm.common.delta.DeltaType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.framework.DtmCalciteFramework;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import ru.ibs.dtm.query.calcite.core.service.impl.DeltaQueryPreprocessorImpl;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DeltaQueryPreprocessorImplTest {

    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig;
    private final DefinitionService<SqlNode> definitionService = mock(CalciteDefinitionService.class);
    private final DeltaService deltaService = mock(DeltaService.class);
    private DeltaQueryPreprocessor deltaQueryPreprocessor;
    private Planner planner;

    @BeforeEach
    void setUp() {
        parserConfig = SqlParser.configBuilder()
                .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        deltaQueryPreprocessor = new DeltaQueryPreprocessorImpl(definitionService, deltaService);
    }

    @Test
    void processWithDeltaNums() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        List<Long> deltas = Arrays.asList(1L, 2L, 3L, 3L);
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
                new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                        1L, null, DeltaType.NUM, "test_datamart", "tblc", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        3L, null, DeltaType.NUM, "test", "tbl", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        3L, null, DeltaType.NUM, "test2", "tblx", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        3L, null, DeltaType.NUM, "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        request.setEnvName("local");
        request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Long>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltas));
            return null;
        }).when(deltaService).getDeltasOnDateTimes(any(), any());

        deltaQueryPreprocessor.process(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        assertNotNull(promise.future().result());
        assertEquals(4, ((QueryRequest) promise.future().result()).getDeltaInformations().size());
    }

    @Test
    void processWithDeltaNumIntervals() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF DELTA_NUM 1 t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        List<Long> deltas = new ArrayList<>();
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
                new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                        1L, null, DeltaType.NUM, "test_datamart", "tblc", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        2L, null, DeltaType.NUM, "test", "tbl", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        null, new DeltaInterval(3L, 4L), DeltaType.STARTED_IN, "test2", "tblx", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        null, new DeltaInterval(3L, 4L), DeltaType.FINISHED_IN, "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        request.setEnvName("local");
        request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Long>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltas));
            return null;
        }).when(deltaService).getDeltasOnDateTimes(any(), any());

        deltaQueryPreprocessor.process(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        assertNotNull(promise.future().result());
        assertEquals(4, ((QueryRequest) promise.future().result()).getDeltaInformations().size());
        assertEquals(Arrays.asList(1L, 2L), ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                .filter(d -> d.getType().equals(DeltaType.NUM))
                .map(DeltaInformation::getDeltaNum).collect(Collectors.toList()));
        assertEquals(Collections.singletonList(new DeltaInterval(3L, 4L)),
                ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                        .filter(d -> d.getType().equals(DeltaType.STARTED_IN))
                        .map(DeltaInformation::getDeltaInterval).collect(Collectors.toList()));
        assertEquals(Collections.singletonList(new DeltaInterval(3L, 4L)),
                ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                        .filter(d -> d.getType().equals(DeltaType.FINISHED_IN))
                        .map(DeltaInformation::getDeltaInterval).collect(Collectors.toList()));
    }

    @Test
    void processWithDeltaNumAndIntervals() throws SqlParseException {
        Promise promise = Promise.promise();
        val sql = "SELECT v.col1 AS c, (SELECT col4 FROM tblc FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59' t3 WHERE tblx.col6 = 0 ) AS r\n" +
                "FROM test.tbl FOR SYSTEM_TIME AS OF DELTA_NUM 2 AS t\n" +
                "INNER JOIN (SELECT col4, col5\n" +
                "FROM test2.tblx FOR SYSTEM_TIME STARTED IN (3,4)\n" +
                "WHERE tblx.col6 = 0) AS v ON t.col3 = v.col4\n" +
                "WHERE EXISTS (SELECT id\n" +
                "FROM (SELECT col4, col5 FROM tblz FOR SYSTEM_TIME FINISHED IN (3,4) WHERE tblz.col6 = 0) AS view) order by v.col1";
        SqlNode sqlNode = planner.parse(sql);
        List<Long> deltas = Arrays.asList(1L, 2L);
        final SqlParserPos pos = new SqlParserPos(0, 0);
        List<DeltaInformation> deltaInfoList = Arrays.asList(
                new DeltaInformation("t3", "2018-07-29 23:59:59", false,
                        null, null, DeltaType.NUM, "test_datamart", "tblc", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        2L, null, DeltaType.NUM, "test", "tbl", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        null, new DeltaInterval(3L, 4L), DeltaType.STARTED_IN, "test2", "tblx", pos),
                new DeltaInformation("", "2019-12-23 15:15:14", false,
                        null, new DeltaInterval(3L, 4L), DeltaType.FINISHED_IN, "test_datamart", "tblz", pos)
        );

        QueryRequest request = new QueryRequest();
        request.setDatamartMnemonic("test_datamart");
        request.setDeltaInformations(deltaInfoList);
        request.setRequestId(UUID.randomUUID());
        request.setEnvName("local");
        request.setSourceType(SourceType.ADB);
        request.setSql(sql);
        when(definitionService.processingQuery(any())).thenReturn(sqlNode);

        Mockito.doAnswer(invocation -> {
            final Handler<AsyncResult<List<Long>>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(deltas));
            return null;
        }).when(deltaService).getDeltasOnDateTimes(any(), any());

        deltaQueryPreprocessor.process(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                });

        assertNotNull(promise.future().result());
        assertEquals(4, ((QueryRequest) promise.future().result()).getDeltaInformations().size());
        assertEquals(deltas, ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                .filter(d -> d.getType().equals(DeltaType.NUM))
                .map(DeltaInformation::getDeltaNum).collect(Collectors.toList()));
        assertEquals(Collections.singletonList(new DeltaInterval(3L, 4L)),
                ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                .filter(d -> d.getType().equals(DeltaType.STARTED_IN))
                .map(DeltaInformation::getDeltaInterval).collect(Collectors.toList()));
        assertEquals(Collections.singletonList(new DeltaInterval(3L, 4L)),
                ((QueryRequest) promise.future().result()).getDeltaInformations().stream()
                        .filter(d -> d.getType().equals(DeltaType.FINISHED_IN))
                        .map(DeltaInformation::getDeltaInterval).collect(Collectors.toList()));
    }
}
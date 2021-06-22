package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaterializedViewReplacerTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();

    @Spy
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    @Mock
    private DeltaInformationExtractor deltaInformationExtractor;

    @Mock
    private DeltaInformationService deltaInformationService;

    @InjectMocks
    private MaterializedViewReplacer viewReplacer;

    @Mock
    private ViewReplacerService viewReplacerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testViewNotReplacedWhenNoHints() {
        String sql = "SELECT * FROM datamart.mat_view";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                false,
                DeltaType.WITHOUT_SNAPSHOT,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        viewReplacer.replace(context)
                .onSuccess(result -> verify(viewReplacerService, never()).replace(any()))
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewReplacedByDateTime() {
        String sql = "SELECT * FROM datamart.mat_view FOR SYSTEM_TIME AS OF '2021-06-22 11:42:00'";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .entity(new Entity().toBuilder()
                        .viewQuery("SELECT * FROM datamart.some_table")
                        .materializedDeltaNum(1L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                "'2021-06-22 11:42:00'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        // Delta of mat view (1) is less than delta from the request. Replacing a view
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService).getDeltaNumByDatetime(context.getDatamart(), CalciteUtil.parseLocalDateTime("2021-06-22 11:42:00"));
        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));
        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService).replace(any(ViewReplaceContext.class));
                    SqlNode viewQueryNode = context.getViewQueryNode();
                    assertThat(viewQueryNode.toString()).isEqualToNormalizingNewlines("SELECT *\nFROM `datamart`.`some_table`");
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewNotReplacedByDateTime() {
        String sql = "SELECT * FROM datamart.mat_view FOR SYSTEM_TIME AS OF '2021-06-22 11:42:00'";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .datamart("datamart")
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .entity(new Entity().toBuilder()
                        .viewQuery("SELECT * FROM datamart.some_table")
                        .materializedDeltaNum(5L)
                        .build())
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                "'2021-06-22 11:42:00'",
                false,
                DeltaType.DATETIME,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);
        // Delta of mat view (10) is greater or equal than delta from the request. NOT replacing a view
        doAnswer(answer -> Future.succeededFuture(5L)).when(deltaInformationService).getDeltaNumByDatetime(context.getDatamart(), CalciteUtil.parseLocalDateTime("2021-06-22 11:42:00"));
        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));
        viewReplacer.replace(context)
                .onComplete(result -> {
                    verify(viewReplacerService, never()).replace(any(ViewReplaceContext.class));
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testLatestUncommittedDeltaIsNotSupported() {
        String sql = "SELECT * FROM datamart.mat_view";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .build();

        val deltaInformation = new DeltaInformation(
                "",
                null,
                true,
                DeltaType.NUM,
                null,
                null,
                "datamart",
                "mat_view",
                null
        );

        when(deltaInformationExtractor.getDeltaInformation(any(), any()))
                .thenReturn(deltaInformation);

        boolean thrown = false;
        try {
            viewReplacer.replace(context);
        } catch (DtmException e) {
            thrown = true;
            verify(viewReplacerService, never()).replace(any());
        }

        if (!thrown) {
            fail("DtmException was expected");
        }
    }

}

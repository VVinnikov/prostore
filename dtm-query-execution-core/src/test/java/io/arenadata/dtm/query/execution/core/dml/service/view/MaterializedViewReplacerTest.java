package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MaterializedViewReplacerTest {

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();

    @Spy
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory()));

    @InjectMocks
    private MaterializedViewReplacer viewReplacer;

    @Mock
    private ViewReplacerService viewReplacerService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testViewNotReplaced() {
        String sql = "SELECT * FROM datamart.some_view";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .build();

        viewReplacer.replace(context)
                .onSuccess(result -> verify(viewReplacerService, never()).replace(any()))
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

    @Test
    public void testViewReplaced() {
        String sql = "SELECT * FROM datamart.some_view FOR SYSTEM_TIME AS OF LATEST_UNCOMMITTED_DELTA";
        SqlNode viewQuery = definitionService.processingQuery(sql);

        ViewReplaceContext context = ViewReplaceContext.builder()
                .viewReplacerService(viewReplacerService)
                .allNodes(new SqlSelectTree(viewQuery))
                .entity(new Entity().toBuilder().viewQuery("SELECT * FROM datamart.some_table").build())
                .build();

        doAnswer(answer -> Future.succeededFuture()).when(viewReplacerService).replace(any(ViewReplaceContext.class));
        viewReplacer.replace(context)
                .onSuccess(result -> {
                    verify(viewReplacerService).replace(any(ViewReplaceContext.class));
                    SqlNode viewQueryNode = context.getViewQueryNode();
                    assertThat(viewQueryNode.toString()).isEqualToNormalizingNewlines("SELECT *\nFROM `datamart`.`some_table`");
                })
                .onFailure(result -> Assert.fail("Error while replacing materialized view"));
    }

}

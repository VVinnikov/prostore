package io.arenadata.dtm.query.execution.core.calcite.edml;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.edml.SqlRollbackCrashedWriteOps;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlEdmlParserImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(
                    calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void parseRollbackCrashedWriteOpsSuccess() {
        SqlNode sqlNode = definitionService.processingQuery("ROLLBACK CRASHED_WRITE_OPERATIONS");
        assertTrue(sqlNode instanceof SqlRollbackCrashedWriteOps);
    }

    @Test
    void parseRollbackCrashedWriteOpsError() {
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("ROLLBACK CRASHED_WRITE_OPERATIONS()");
        });
    }

}

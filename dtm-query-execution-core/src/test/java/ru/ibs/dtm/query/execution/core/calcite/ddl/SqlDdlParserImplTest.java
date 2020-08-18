package ru.ibs.dtm.query.execution.core.calcite.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class SqlDdlParserImplTest {
    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(
                    calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void parseAlter() {
        SqlNode sqlNode = definitionService.processingQuery("ALTER VIEW test.view_a AS SELECT * FROM test.tab_1");
        assertTrue(sqlNode instanceof SqlAlterView);
        assertEquals(Arrays.asList("test", "view_a"),
                ((SqlIdentifier) ((SqlAlterView) sqlNode).getOperandList().get(0)).names);
        assertEquals(3, ((SqlAlterView) sqlNode).getOperandList().size());
    }

    @Test
    void parseAlterWithoutFromClause() {
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("ALTER VIEW test.view_a AS SELECT * ");
        });
    }

    @Test
    void parseCreateViewSuccess() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW test.view_a AS SELECT * FROM test.tab_1");
        assertTrue(sqlNode instanceof SqlCreateView);
    }

    @Test
    void parseCreateViewWithoutFromClause() {
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("CREATE VIEW test.view_a AS SELECT * ft");
        });
    }

}

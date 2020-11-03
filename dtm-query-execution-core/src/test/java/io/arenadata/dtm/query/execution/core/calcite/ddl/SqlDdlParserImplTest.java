package io.arenadata.dtm.query.execution.core.calcite.ddl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateView;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlUseSchema;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlDdlParserImplTest {
    private static final String CREATE_TABLE_QUERY = "create table test.table_name\n" +
            "(\n" +
            "    account_id bigint,\n" +
            "    account_type varchar(1), -- D/C (дебет/кредит)\n" +
            "    primary key (account_id)\n" +
            ") distributed by (account_id)";
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
    void parseUseSchema() {
        SqlNode sqlNode = definitionService.processingQuery("USE shares");
        assertTrue(sqlNode instanceof SqlUseSchema);
        assertEquals("shares", ((SqlIdentifier) ((SqlUseSchema) sqlNode).getOperandList().get(0)).names.get(0));
        assertEquals("USE shares", sqlNode.toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT)).toString());
    }

    @Test
    void parseUseSchemaWithQuotes() {
        SqlNode sqlNode = definitionService.processingQuery("USE \"shares\"");
        assertTrue(sqlNode instanceof SqlUseSchema);
        assertEquals("shares", ((SqlIdentifier) ((SqlUseSchema) sqlNode).getOperandList().get(0)).names.get(0));
        assertEquals("USE shares", sqlNode.toSqlString(new SqlDialect(SqlDialect.EMPTY_CONTEXT)).toString());
    }

    @Test
    void parseIncorrectUseSchema() {
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("USEshares");
        });
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("USE shares t");
        });
        assertThrows(SqlParseException.class, () -> {
            definitionService.processingQuery("USE 'shares'");
        });
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

    @Test
    void createTable() {
        createTable(CREATE_TABLE_QUERY);
    }

    @Test
    void createTableWithDestination() {
        Set<SourceType> selectedSourceTypes = new HashSet<>();
        selectedSourceTypes.add(SourceType.ADB);
        selectedSourceTypes.add(SourceType.ADG);
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)",
                selectedSourceTypes.stream().map(SourceType::name).collect(Collectors.joining(", ")));
        createTable(query, sqlCreateTable -> assertEquals(selectedSourceTypes, sqlCreateTable.getDestination()));
    }

    @Test
    void createTableWithInformationSchema() {
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)",
                SourceType.INFORMATION_SCHEMA.name());
        assertThrows(SqlParseException.class, () -> createTable(query));
    }

    @Test
    void createTableWithInvalidDestination() {
        String query = String.format(CREATE_TABLE_QUERY + " DATASOURCE_TYPE (%s)", "adcvcb");
        assertThrows(SqlParseException.class, () -> createTable(query));
    }

    void createTable(String query) {
        createTable(query, sqlCreateTable -> {});
    }

    void createTable(String query, Consumer<SqlCreateTable> consumer) {
        SqlNode sqlNode = definitionService.processingQuery(query);
        assertTrue(sqlNode instanceof SqlCreateTable);
        SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
        assertEquals(Arrays.asList("test", "table_name"),
                ((SqlIdentifier) sqlCreateTable.getOperandList().get(0)).names);
        consumer.accept(sqlCreateTable);
    }
}

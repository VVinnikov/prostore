package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteSchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.CalciteConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.impl.SchemaFactoryImpl;

import static org.junit.jupiter.api.Assertions.assertNotNull;

// Class to verify how query parts are represented in the Calcite
public class CalciteParserTest {

    private static CalciteContextProvider calciteContextProvider;

    @BeforeAll
    public static void setup() {
        CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
        calciteConfiguration.init();
        SqlParser.Config parserConfig = calciteConfiguration.configDdlParser(
                calciteConfiguration.ddlParserImplFactory()
        );

        calciteContextProvider = new CalciteContextProvider(
                parserConfig,
                new CalciteSchemaFactory(new SchemaFactoryImpl()));
    }

    @SneakyThrows
    @Test
    public void testExistsQuery() {
        CalciteContext ctx = calciteContextProvider.context(null);
        String query = "select 1 from tbl1_actual t /*final*/ where sign < 0 limit 1";
        SqlNode root = ctx.getPlanner().parse(query);
        assertNotNull(root);
    }

}

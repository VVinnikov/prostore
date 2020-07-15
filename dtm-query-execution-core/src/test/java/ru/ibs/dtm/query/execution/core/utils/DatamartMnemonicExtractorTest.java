package ru.ibs.dtm.query.execution.core.utils;

import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.service.impl.CoreCalciteDefinitionService;

import static org.junit.jupiter.api.Assertions.*;

class DatamartMnemonicExtractorTest {
    public static final String EXPECTED_DATAMART = "test";
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private final DatamartMnemonicExtractor extractor = new DatamartMnemonicExtractor();

    @Test
    void extractFromSelect() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromSelectWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            SqlNode sqlNode = definitionService.processingQuery("select * from tbl1");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromSelectSnapshot() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
        assertTrue(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql().contains("FOR SYSTEM_TIME AS OF "));
    }

    @Test
    void extractFromSelectSnapshotWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("select * from tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromInnerSelect() {
        val sqlNode = definitionService.processingQuery("select * from (select id from test.tbl1) AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromInnerSelectSnapshot() {
        val sqlNode = definitionService.processingQuery("select * from (select * from test.tbl1" +
                " FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14') AS t");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromJoin() {
        val sqlNode = definitionService.processingQuery("select * from test.tbl1 " +
                "JOIN test.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateTable() {
        val sqlNode = definitionService.processingQuery("CREATE TABLE test.table_name " +
                "(col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) )" +
                " DISTRIBUTED BY (col1, col2)");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateTableWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery(
                    "CREATE TABLE table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) )" +
                            " DISTRIBUTED BY (col1, col2)"
            );
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromDropTable() {
        val sqlNode = definitionService.processingQuery("DROP TABLE test.table_name");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromDropTableWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("DROP TABLE table_name");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromCreateView() {
        val sqlNode = definitionService.processingQuery("CREATE VIEW test.view1 as select * from test2.tbl1" +
                " JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateOrReplaceView() {
        val sqlNode = definitionService.processingQuery("CREATE OR REPLACE VIEW test.view1 as select * from test2.tbl1" +
                " JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }

    @Test
    void extractFromCreateViewWithoutDatamart() {
        assertThrows(IllegalArgumentException.class, () -> {
            val sqlNode = definitionService.processingQuery("CREATE VIEW view1 as select * from test2.tbl1 " +
                    "JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
            extractor.extract(sqlNode);
        });
    }

    @Test
    void extractFromInsert() {
        val sqlNode = definitionService.processingQuery("INSERT INTO test.PSO SELECT * FROM test.PSO");
        val datamart = extractor.extract(sqlNode);
        assertEquals(EXPECTED_DATAMART, datamart);
    }
}

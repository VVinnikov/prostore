package ru.ibs.dtm.query.execution.core.utils;

import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
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
        SqlNode sqlNode = definitionService.processingQuery("select * from test.tbl1");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromSelectWithoutDatamart() {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromSelectSnapshot() {
        SqlNode sqlNode = definitionService.processingQuery("select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromSelectSnapshotWithoutDatamart() {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromInnerSelect() {
        SqlNode sqlNode = definitionService.processingQuery("select * from (select id from test.tbl1) AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromInnerSelectSnapshot() {
        SqlNode sqlNode = definitionService.processingQuery("select * from (select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14') AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromJoin() {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1 JOIN test.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateTable() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE TABLE test.table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) ) DISTRIBUTED BY (col1, col2)");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateTableWithoutDatamart() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE TABLE table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) ) DISTRIBUTED BY (col1, col2)");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromDropTable() {
        SqlNode sqlNode = definitionService.processingQuery("DROP TABLE test.table_name");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromDropTableWithoutDatamart() {
        SqlNode sqlNode = definitionService.processingQuery("DROP TABLE table_name");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromCreateView() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW test.view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateOrReplaceView() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE OR REPLACE VIEW test.view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateViewWithoutDatamart() {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromInsert() {
        SqlNode sqlNode = definitionService.processingQuery("INSERT INTO test.PSO SELECT * FROM test.PSO");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }
}

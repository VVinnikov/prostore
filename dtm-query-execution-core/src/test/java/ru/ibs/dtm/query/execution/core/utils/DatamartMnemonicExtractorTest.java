package ru.ibs.dtm.query.execution.core.utils;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.core.service.impl.CalciteDefinitionService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DatamartMnemonicExtractorTest {
    public static final String EXPECTED_DATAMART = "test";
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CalciteDefinitionService(config.configEddlParser(config.eddlParserImplFactory()));
    private final DatamartMnemonicExtractor extractor = new DatamartMnemonicExtractor();

    @Test
    void extractFromSelect() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from test.tbl1");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromSelectWithoutDatamart() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromSelectSnapshot() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromSelectSnapshotWithoutDatamart() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14' AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromInnerSelect() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from (select id from test.tbl1) AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromInnerSelectSnapshot() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from (select * from test.tbl1 FOR SYSTEM_TIME AS OF '2019-12-23 15:15:14') AS t");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromJoin() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("select * from tbl1 JOIN test.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateTable() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("CREATE TABLE test.table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) ) DISTRIBUTED BY (col1, col2)");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateTableWithoutDatamart() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("CREATE TABLE table_name (col1 datatype1, col2 datatype2, PRIMARY KEY (col1, col2) ) DISTRIBUTED BY (col1, col2)");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromDropTable() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("DROP TABLE test.table_name");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromDropTableWithoutDatamart() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("DROP TABLE table_name");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromCreateView() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW test.view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateOrReplaceView() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("CREATE OR REPLACE VIEW test.view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }

    @Test
    void extractFromCreateViewWithoutDatamart() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("CREATE VIEW view1 as select * from test2.tbl1 JOIN test2.view FOR SYSTEM_TIME AS OF '2018-07-29 23:59:59'");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertFalse(datamartOpt.isPresent());
    }

    @Test
    void extractFromInsert() throws Exception {
        SqlNode sqlNode = definitionService.processingQuery("INSERT INTO test.PSO SELECT * FROM test.PSO");
        Optional<String> datamartOpt = extractor.extract(sqlNode);

        assertTrue(datamartOpt.isPresent());
        assertEquals(EXPECTED_DATAMART, datamartOpt.get());
    }
}

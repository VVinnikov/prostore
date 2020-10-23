package io.arenadata.dtm.query.execution.core.calcite.delta.parser;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.eddl.FormatOperator;
import io.arenadata.dtm.query.calcite.core.extension.eddl.LocationOperator;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDownloadExternalTable;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlNodeUtils;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlDeltaDdlParserImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    public void testBeginDeltaWithNum() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse("BEGIN DELTA SET 1");
        assertTrue(sqlNode instanceof SqlBeginDelta);
    }

    @Test
    public void testBeginDeltaDefault() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse("BEGIN DELTA");
        assertTrue(sqlNode instanceof SqlBeginDelta);
    }

    @Test
    public void testCommitDeltaWithDateTime() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse("COMMIT DELTA SET '2020-06-11T10:00:00'");
        assertTrue(sqlNode instanceof SqlCommitDelta);
    }

    @Test
    public void testCommitDeltaDefault() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse("COMMIT DELTA");
        assertTrue(sqlNode instanceof SqlCommitDelta);
    }

    @Test
    public void testCreateDownloadExtTableInvalidSql() {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidTypeLocationOperator() {

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic1=test' FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidFormat() {

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic=test' FORMAT 'avro1'"));
    }

    @Test
    public void testCreateDownloadExtTableOmitChunkSize() throws SqlParseException {

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'");
        assertTrue(sqlNode instanceof SqlCreateDownloadExternalTable);

        SqlCreateDownloadExternalTable sqlCreateDownloadExternalTable = (SqlCreateDownloadExternalTable) sqlNode;
        assertEquals("s",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, SqlIdentifier.class).getSimple());
        assertEquals(Type.KAFKA_TOPIC,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getType());
        assertEquals("kafka://zookeeper_host:port/topic",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getLocation());
        assertEquals(Format.AVRO,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, FormatOperator.class).getFormat());
    }

    @Test
    void parseDdlWithQuote() throws SqlParseException {
        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);

        SqlCreateTable node =
                (SqlCreateTable) planner.parse("CREATE TABLE a(\"index\" integer)");
        assertTrue(node instanceof SqlCreateTable);
        assertEquals("a", SqlNodeUtils.getOne(node, SqlIdentifier.class).getSimple());
        assertEquals("index",
                SqlNodeUtils.getOne(
                        (SqlColumnDeclaration) SqlNodeUtils.getOne(node, SqlNodeList.class).getList().get(0),
                        SqlIdentifier.class).getSimple());
    }
}

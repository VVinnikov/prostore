package ru.ibs.dtm.query.execution.core.calcite.eddl.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.ibs.dtm.query.calcite.core.extension.eddl.*;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SqlEddlParserImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
            calciteCoreConfiguration.eddlParserImplFactory()
    );

    @Test
    public void testDropDownloadExtTable() throws SqlParseException {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("DROP DOWNLOAD EXTERNAL TABLE s");

        assertTrue(sqlNode instanceof SqlDropDownloadExternalTable);
    }

    @Test
    public void testCreateDownloadExtTable() throws SqlParseException {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) LOCATION 'kafka://zookeeper_host:port/topic_UPPER_case' FORMAT 'avro' CHUNK_SIZE 10");
        assertTrue(sqlNode instanceof SqlCreateDownloadExternalTable);
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "integer");
        columns.put("name", "varchar");

        SqlCreateDownloadExternalTable sqlCreateDownloadExternalTable = (SqlCreateDownloadExternalTable) sqlNode;
        SqlNodeList columnList = (SqlNodeList) sqlCreateDownloadExternalTable.getOperandList().get(1);
        assertEquals("s",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, SqlIdentifier.class).getSimple());
        assertEquals("id", ((SqlIdentifier) ((SqlColumnDeclaration) columnList.get(0)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("id"), ((SqlDataTypeSpec) ((SqlColumnDeclaration)columnList.get(0))
                .getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals("name", ((SqlIdentifier) ((SqlColumnDeclaration) columnList.get(1)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("name"), ((SqlDataTypeSpec) ((SqlColumnDeclaration) columnList.get(1))
                .getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals(Type.KAFKA_TOPIC,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getType());
        assertEquals("kafka://zookeeper_host:port/topic_UPPER_case",
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, LocationOperator.class).getLocation());
        assertEquals(Format.AVRO,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, FormatOperator.class).getFormat());
        assertEquals(10,
                SqlNodeUtils.getOne(sqlCreateDownloadExternalTable, ChunkSizeOperator.class).getChunkSize());
    }

    @Test
    public void testCreateDownloadExtTableInvalidSql() {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidTypeLocationOperator() {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) LOCATION 'kafkaTopic1=test' FORMAT 'avro'"));
    }

    @Test
    public void testCreateDownloadExtTableInvalidFormat() {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        assertThrows(SqlParseException.class,
                () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s (id integer, name varchar(100)) LOCATION 'kafkaTopic=test' FORMAT 'avro1'"));
    }

    @Test
    public void testCreateDownloadExtTableOmitChunkSize() throws SqlParseException {

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

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
    public void testCreateUploadExtTableWithoutMessageLimit() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "integer");
        columns.put("name", "varchar");

        SqlNode sqlNode = planner.parse("CREATE UPLOAD EXTERNAL TABLE uplExtTab (id integer not null, name varchar(100), primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'");
        assertTrue(sqlNode instanceof SqlCreateUploadExternalTable);
        SqlCreateUploadExternalTable sqlCreateUploadExternalTable = (SqlCreateUploadExternalTable) sqlNode;
        assertEquals("uplExtTab".toLowerCase(),
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, SqlIdentifier.class).getSimple());
        assertEquals("id", ((SqlIdentifier) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(0)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("id"), ((SqlDataTypeSpec) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(0)).getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals("name", ((SqlIdentifier) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(1)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("name"), ((SqlDataTypeSpec) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(1)).getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals(SqlKind.PRIMARY_KEY, ((SqlCreateUploadExternalTable) sqlNode).getColumnList().get(2).getKind());

        assertEquals(Type.KAFKA_TOPIC,
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, LocationOperator.class).getType());
        assertEquals("kafka://zookeeper_host:port/topic",
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, LocationOperator.class).getLocation());
        assertEquals(Format.AVRO,
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, FormatOperator.class).getFormat());
        assertNull(SqlNodeUtils.getOne((SqlCall) sqlNode, MassageLimitOperator.class).getMessageLimit());
    }

    @Test
    public void testCreateUploadExtTable() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "integer");
        columns.put("name", "varchar");

        SqlNode sqlNode = planner.parse("CREATE UPLOAD EXTERNAL TABLE uplExtTab (id integer not null, name varchar(100), primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro' MESSAGE_LIMIT 1000");
        assertTrue(sqlNode instanceof SqlCreateUploadExternalTable);
        SqlCreateUploadExternalTable sqlCreateUploadExternalTable = (SqlCreateUploadExternalTable) sqlNode;
        assertEquals("uplExtTab".toLowerCase(),
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, SqlIdentifier.class).getSimple());
        assertEquals("id", ((SqlIdentifier) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(0)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("id"), ((SqlDataTypeSpec) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(0)).getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals("name", ((SqlIdentifier) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(1)).getOperandList().get(0)).getSimple());
        assertEquals(columns.get("name"), ((SqlDataTypeSpec) ((SqlColumnDeclaration) sqlCreateUploadExternalTable.getColumnList()
                .get(1)).getOperandList().get(1)).getTypeName().getSimple().toLowerCase());
        assertEquals(SqlKind.PRIMARY_KEY, ((SqlCreateUploadExternalTable) sqlNode).getColumnList().get(2).getKind());
        assertEquals(Type.KAFKA_TOPIC,
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, LocationOperator.class).getType());
        assertEquals("kafka://zookeeper_host:port/topic",
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, LocationOperator.class).getLocation());
        assertEquals(Format.AVRO,
                SqlNodeUtils.getOne(sqlCreateUploadExternalTable, FormatOperator.class).getFormat());
        assertEquals(1000, SqlNodeUtils.getOne((SqlCall) sqlNode, MassageLimitOperator.class).getMessageLimit());
    }

    @Test
    public void testDropUploadExtTable() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse("DROP UPLOAD EXTERNAL TABLE s");
        assertTrue(sqlNode instanceof SqlDropUploadExternalTable);
    }

    @Test
    void parseDdlWithQuote() throws SqlParseException {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlCreateTable node = (SqlCreateTable) planner.parse("CREATE TABLE a(\"index\" integer)");
        assertTrue(node instanceof SqlCreateTable);
        assertEquals("a", SqlNodeUtils.getOne(node, SqlIdentifier.class).getSimple());
        assertEquals("index",
                SqlNodeUtils.getOne(
                        (SqlColumnDeclaration) SqlNodeUtils.getOne(node, SqlNodeList.class).getList().get(0),
                        SqlIdentifier.class).getSimple());
    }
}

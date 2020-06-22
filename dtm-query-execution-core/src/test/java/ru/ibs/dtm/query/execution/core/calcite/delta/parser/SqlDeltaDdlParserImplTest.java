package ru.ibs.dtm.query.execution.core.calcite.delta.parser;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlBeginDelta;
import ru.ibs.dtm.query.execution.core.calcite.delta.SqlCommitDelta;
import ru.ibs.dtm.query.execution.core.calcite.eddl.*;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;

import static org.junit.jupiter.api.Assertions.*;

public class SqlDeltaDdlParserImplTest {

  private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
  private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(
    calciteConfiguration.eddlParserImplFactory()
  );

  @Test
  public void testBeginDeltaWithNum() throws SqlParseException {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);
    SqlNode sqlNode = planner.parse("BEGIN DELTA SET 1");
    assertTrue(sqlNode instanceof SqlBeginDelta);
  }

  @Test
  public void testBeginDeltaDefault() throws SqlParseException {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);
    SqlNode sqlNode = planner.parse("BEGIN DELTA");
    assertTrue(sqlNode instanceof SqlBeginDelta);
  }

  @Test
  public void testCommitDeltaWithDateTime() throws SqlParseException {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);
    SqlNode sqlNode = planner.parse("COMMIT DELTA SET '2020-06-11T10:00:00'");
    assertTrue(sqlNode instanceof SqlCommitDelta);
  }

  @Test
  public void testCommitDeltaDefault() throws SqlParseException {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);
    SqlNode sqlNode = planner.parse("COMMIT DELTA");
    assertTrue(sqlNode instanceof SqlCommitDelta);
  }

  @Test
  public void testCreateDownloadExtTableInvalidSql() {

    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    assertThrows(SqlParseException.class,
      () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s FORMAT 'avro'"));
  }

  @Test
  public void testCreateDownloadExtTableInvalidTypeLocationOperator() {

    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    assertThrows(SqlParseException.class,
      () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic1=test' FORMAT 'avro'"));
  }

  @Test
  public void testCreateDownloadExtTableInvalidFormat() {

    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    assertThrows(SqlParseException.class,
      () -> planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafkaTopic=test' FORMAT 'avro1'"));
  }

  @Test
  public void testCreateDownloadExtTableOmitChunkSize() throws SqlParseException {

    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    SqlNode sqlNode = planner.parse("CREATE DOWNLOAD EXTERNAL TABLE s LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'");
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

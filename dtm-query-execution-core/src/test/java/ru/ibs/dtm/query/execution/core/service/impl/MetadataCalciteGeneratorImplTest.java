package ru.ibs.dtm.query.execution.core.service.impl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.metadata.impl.MetadataCalciteGeneratorImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataCalciteGeneratorImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private MetadataCalciteGenerator metadataCalciteGenerator;
    private ClassTable table;
    private ClassTable table2;

    @BeforeEach
    void setUp() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = Frameworks.getPlanner(frameworkConfig);
        metadataCalciteGenerator = new MetadataCalciteGeneratorImpl();
        final List<ClassField> fields = createFieldsForUplTable();
        final List<ClassField> fields2 = createFieldsForTable();
        table = new ClassTable("uplexttab", null, fields);
        table2 = new ClassTable("accounts", "shares", fields2);
    }

    private List<ClassField> createFieldsForTable() {
        ClassField f1 = new ClassField(0, "id", ColumnType.INT, false, true);
        f1.setPrimaryOrder(1);
        ClassField f2 = new ClassField(1, "name", ColumnType.VARCHAR, true, false);
        f2.setSize(100);
        ClassField f3 = new ClassField(2, "account_id", ColumnType.INT, false, true);
        f1.setPrimaryOrder(1);
        f3.setPrimaryOrder(2);
        f3.setShardingOrder(1);
        return new ArrayList<>(Arrays.asList(f1, f2, f3));
    }

    private List<ClassField> createFieldsForUplTable() {
        ClassField f1 = new ClassField(0,"id", ColumnType.INT, false, true);
        f1.setPrimaryOrder(1);
        ClassField f2 = new ClassField(1,"name", ColumnType.VARCHAR, true, false);
        f2.setSize(100);
        ClassField f3 = new ClassField(2,"booleanvalue", ColumnType.BOOLEAN, true, false);
        ClassField f4 = new ClassField(3,"charvalue", ColumnType.CHAR, true, false);
        ClassField f5 = new ClassField(4,"bgintvalue", ColumnType.BIGINT, true, false);
        ClassField f6 = new ClassField(5,"dbvalue", ColumnType.DOUBLE, true, false);
        ClassField f7 = new ClassField(6,"flvalue", ColumnType.FLOAT, true, false);
        ClassField f8 = new ClassField(7,"datevalue", ColumnType.DATE, true, false);
        ClassField f9 = new ClassField(8,"timevalue", ColumnType.TIME, true, false);
        ClassField f11 = new ClassField(9, "tsvalue", ColumnType.TIMESTAMP, true, false);
        f11.setAccuracy(10);
        return new ArrayList<>(Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9, f11));
    }

    @Test
    void generateTableMetadataWithoutSchema() throws SqlParseException {
        String sql = "CREATE UPLOAD EXTERNAL TABLE uplExtTab (" +
                "id integer not null," +
                " name varchar(100)," +
                " booleanValue boolean, " +
                " charValue char, " +
                " bgIntValue bigint, " +
                " dbValue double, " +
                " flValue float, " +
                " dateValue date, " +
                " timeValue time, " +
                " tsValue timestamp(10), " +
                " primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'";
        SqlNode sqlNode = planner.parse(sql);
        ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table, classTable);
    }

    @Test
    void generateTableMetadataWithSchema() throws SqlParseException {
        String sql = "CREATE UPLOAD EXTERNAL TABLE test_datamart.uplExtTab (" +
                "id integer not null," +
                " name varchar(100)," +
                " booleanValue boolean, " +
                " charValue char, " +
                " bgIntValue bigint, " +
                " dbValue double, " +
                " flValue float, " +
                " dateValue date, " +
                " timeValue time, " +
                " tsValue timestamp(10), " +
                " primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'";
        SqlNode sqlNode = planner.parse(sql);
        table.setSchema("test_datamart");
        ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table, classTable);
    }

    @Test
    void generateTableMetadata() throws SqlParseException {
        String sql = "create table shares.accounts (id integer not null, name varchar(100)," +
                " account_id integer not null, primary key(id, account_id)) distributed by (account_id)";
        SqlNode sqlNode = planner.parse(sql);
        ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table2, classTable);
    }

}

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
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import ru.ibs.dtm.query.execution.core.service.MetadataCalciteGenerator;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataCalciteGeneratorImplTest {

    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteConfiguration.eddlParserImplFactory());
    private Planner planner;
    private MetadataCalciteGenerator metadataCalciteGenerator;
    private ClassTable table;

    @BeforeEach
    void setUp() {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = Frameworks.getPlanner(frameworkConfig);
        metadataCalciteGenerator = new MetadataCalciteGeneratorImpl();
        List<ClassField> fields = createFields();
        table = new ClassTable("uplexttab", null, fields);
    }

    private List<ClassField> createFields() {
        ClassField f1 = new ClassField("id", ClassTypes.INT, false, true);
        ClassField f2 = new ClassField("name", ClassTypes.VARCHAR, true, false);
        f2.setSize(100);
        ClassField f3 = new ClassField("booleanvalue", ClassTypes.BOOLEAN, true, false);
        ClassField f4 = new ClassField("charvalue", ClassTypes.CHAR, true, false);
        ClassField f5 = new ClassField("bgintvalue", ClassTypes.BIGINT, true, false);
        ClassField f6 = new ClassField("dbvalue", ClassTypes.DOUBLE, true, false);
        ClassField f7 = new ClassField("flvalue", ClassTypes.FLOAT, true, false);
        ClassField f8 = new ClassField("datevalue", ClassTypes.DATE, true, false);
        ClassField f9 = new ClassField("timevalue", ClassTypes.TIME, true, false);
        ClassField f11 = new ClassField("tsvalue", ClassTypes.TIMESTAMP, true, false);
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
                " tsValue timestamp, " +
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
                " tsValue timestamp, " +
                " primary key(id)) " +
                "LOCATION 'kafka://zookeeper_host:port/topic' FORMAT 'avro'";
        SqlNode sqlNode = planner.parse(sql);
        table.setSchema("test_datamart");
        ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) sqlNode);
        assertEquals(table, classTable);
    }
}
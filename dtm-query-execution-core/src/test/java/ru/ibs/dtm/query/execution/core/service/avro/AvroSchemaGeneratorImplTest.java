package ru.ibs.dtm.query.execution.core.service.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.common.schema.codec.AvroDecoder;
import ru.ibs.dtm.common.schema.codec.AvroEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroSchemaGeneratorImplTest {

    private AvroSchemaGenerator avroSchemaGenerator;
    private AvroEncoder<Object> encoder = new AvroEncoder();
    private ClassTable table;

    @BeforeEach
    void setUp() {
        this.avroSchemaGenerator = new AvroSchemaGeneratorImpl();
        this.table = new ClassTable("uplexttab", "test_datamart", createFields());
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
        ClassField f9 = new ClassField("datetimevalue", ClassTypes.DATETIME, true, false);
        return new ArrayList<>(Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8));
    }

    @Test
    void generateSchemaFields() {
        String avroResult = "{\"type\":\"record\",\"name\":\"uplexttab\",\"namespace\":\"test_datamart\"," +
                "\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null,\"defaultValue\":\"null\"}," +
                "{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]," +
                "\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"booleanvalue\",\"type\":[\"null\",\"boolean\"]," +
                "\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"charvalue\",\"type\":[\"null\"," +
                "{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"}," +
                "{\"name\":\"bgintvalue\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"}," +
                "{\"name\":\"dbvalue\",\"type\":[\"null\",\"double\"],\"default\":null,\"defaultValue\":\"null\"}," +
                "{\"name\":\"flvalue\",\"type\":[\"null\",\"float\"],\"default\":null,\"defaultValue\":\"null\"}," +
                "{\"name\":\"datevalue\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"LocalDate\"}]," +
                "\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table);
        assertEquals(avroResult, tableSchema.toString());
    }

    @Test
    void generateTableSchemaUnsupportedType() {
        table.getFields().add(new ClassField("num", ClassTypes.NUMERIC, true, false));
        Executable executable = () -> avroSchemaGenerator.generateTableSchema(table);
        assertThrows(IllegalArgumentException.class,
                executable, "Unsupported data type: NUMERIC");
    }
}
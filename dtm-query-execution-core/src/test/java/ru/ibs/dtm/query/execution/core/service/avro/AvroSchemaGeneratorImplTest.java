package ru.ibs.dtm.query.execution.core.service.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroSchemaGeneratorImplTest {

    private AvroSchemaGenerator avroSchemaGenerator;
    private ClassTable table;

    @BeforeEach
    void setUp() {
        this.avroSchemaGenerator = new AvroSchemaGeneratorImpl();
        this.table = new ClassTable("uplexttab", "test_datamart", createFields());
    }

    private List<ClassField> createFields() {
        ClassField f1 = new ClassField(0, "id", ColumnType.INT, false, true);
        ClassField f2 = new ClassField(1, "name", ColumnType.VARCHAR, true, false);
        f2.setSize(100);
        ClassField f3 = new ClassField(2, "booleanvalue", ColumnType.BOOLEAN, true, false);
        ClassField f4 = new ClassField(3, "charvalue", ColumnType.CHAR, true, false);
        ClassField f5 = new ClassField(4, "bgintvalue", ColumnType.BIGINT, true, false);
        ClassField f6 = new ClassField(5, "dbvalue", ColumnType.DOUBLE, true, false);
        ClassField f7 = new ClassField(6, "flvalue", ColumnType.FLOAT, true, false);
        ClassField f8 = new ClassField(7, "datevalue", ColumnType.DATE, true, false);
        ClassField f9 = new ClassField(8, "datetimevalue", ColumnType.TIMESTAMP, true, false);
        return new ArrayList<>(Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9));
    }

    @Test
    void generateSchemaFields() {
        String avroResult = "{\"type\":\"record\",\"name\":\"uplexttab\",\"namespace\":\"test_datamart\"," +
            "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}," +
            "{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]," +
            "\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"booleanvalue\",\"type\":[\"null\",\"boolean\"]," +
            "\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"charvalue\",\"type\":[\"null\"," +
            "{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"bgintvalue\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"dbvalue\",\"type\":[\"null\",\"double\"],\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"flvalue\",\"type\":[\"null\",\"float\"],\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"datevalue\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"LocalDate\"}]," +
            "\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"datetimevalue\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"LocalDateTime\"}],\"default\":null,\"defaultValue\":\"null\"}," +
            "{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table);
        assertEquals(avroResult, tableSchema.toString());
    }

    @Test
    void generateTableSchemaUnsupportedType() {
        table.getFields().add(new ClassField(0, "uuid", ColumnType.UUID, true, false));
        Executable executable = () -> avroSchemaGenerator.generateTableSchema(table);
        assertThrows(IllegalArgumentException.class,
            executable, "Unsupported data type: UUID");
    }

    @Test
    void testCheckSysOpFieldAlreadyInFields() {
        table.getFields().add(new ClassField(9, "sys_op", ColumnType.INT, false, false));
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table);
        assertEquals(1, tableSchema.getFields().stream().filter(f -> f.name().equalsIgnoreCase("sys_op")).count());
    }

    @Test
    void testCheckSysOpSkip() {
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table, false);
        assertEquals(0, tableSchema.getFields().stream().filter(f -> f.name().equalsIgnoreCase("sys_op")).count());
    }
}

package io.arenadata.dtm.query.execution.core.service.avro;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.core.base.service.avro.AvroSchemaGenerator;
import io.arenadata.dtm.query.execution.core.base.service.avro.impl.AvroSchemaGeneratorImpl;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroSchemaGeneratorImplTest {

    private AvroSchemaGenerator avroSchemaGenerator;
    private Entity table;

    @BeforeEach
    void setUp() {
        this.avroSchemaGenerator = new AvroSchemaGeneratorImpl();
        this.table = new Entity("uplexttab", "test_datamart", createFields());
    }

    private List<EntityField> createFields() {
        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(100);
        EntityField f3 = new EntityField(2, "booleanvalue", ColumnType.BOOLEAN, true);
        EntityField f4 = new EntityField(3, "charvalue", ColumnType.CHAR, true);
        EntityField f5 = new EntityField(4, "bgintvalue", ColumnType.BIGINT, true);
        EntityField f6 = new EntityField(5, "dbvalue", ColumnType.DOUBLE, true);
        EntityField f7 = new EntityField(6, "flvalue", ColumnType.FLOAT, true);
        EntityField f8 = new EntityField(7, "datevalue", ColumnType.DATE, true);
        EntityField f9 = new EntityField(8, "datetimevalue", ColumnType.TIMESTAMP, true);
        return new ArrayList<>(Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9));
    }

    @Test
    void generateSchemaFields() {
        String avroResult = "{\"type\":\"record\",\"name\":\"uplexttab\",\"namespace\":\"test_datamart\"," +
                "\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\"," +
                "\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}," +
                "{\"name\":\"booleanvalue\",\"type\":[\"null\",\"boolean\"],\"default\":null}," +
                "{\"name\":\"charvalue\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}," +
                "{\"name\":\"bgintvalue\",\"type\":[\"null\",\"long\"],\"default\":null}," +
                "{\"name\":\"dbvalue\",\"type\":[\"null\",\"double\"],\"default\":null}," +
                "{\"name\":\"flvalue\",\"type\":[\"null\",\"float\"],\"default\":null}," +
                "{\"name\":\"datevalue\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null}," +
                "{\"name\":\"datetimevalue\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null}," +
                "{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table);
        assertEquals(avroResult, tableSchema.toString());
    }

    @Test
    void generateTableSchemaUnsupportedType() {
        table.getFields().add(new EntityField(0, "uuid", ColumnType.ANY, true));
        Executable executable = () -> avroSchemaGenerator.generateTableSchema(table);
        assertThrows(IllegalArgumentException.class,
            executable, "Unsupported data type: " + ColumnType.ANY);
    }

    @Test
    void testCheckSysOpFieldAlreadyInFields() {
        table.getFields().add(new EntityField(9, "sys_op", ColumnType.INT, false));
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table);
        assertEquals(1, tableSchema.getFields().stream().filter(f -> f.name().equalsIgnoreCase("sys_op")).count());
    }

    @Test
    void testCheckSysOpSkip() {
        Schema tableSchema = avroSchemaGenerator.generateTableSchema(table, false);
        assertEquals(0, tableSchema.getFields().stream().filter(f -> f.name().equalsIgnoreCase("sys_op")).count());
    }
}

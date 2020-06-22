package ru.ibs.dtm.query.execution.core.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.common.schema.codec.type.LocalDateLogicalType;
import ru.ibs.dtm.common.schema.codec.type.LocalDateTimeLogicalType;

public class AvroUtils {

    public static Schema.Field createSysOpField() {
        return new Schema.Field("sys_op", Schema.create(Schema.Type.INT), null, 0);
    }

    public static Schema.Field toSchemaField(ClassField column) {
        Schema.Field field = new Schema.Field(column.getName(), metadataColumnTypeToAvroSchema(column.getType()), null, Schema.NULL_VALUE);
        field.addProp("defaultValue", "null");
        return field;
    }

    public static Schema metadataColumnTypeToAvroSchema(ClassTypes columnType) {
        Schema schema;
        switch (columnType) {
            case VARCHAR:
            case CHAR:
                schema = Schema.create(Schema.Type.STRING);
                GenericData.setStringType(schema, GenericData.StringType.String);
                break;
            case BIGINT:
                schema = Schema.create(Schema.Type.LONG);
                break;
            case INT:
                schema = Schema.create(Schema.Type.INT);
                break;
            case DOUBLE:
                schema = Schema.create(Schema.Type.DOUBLE);
                break;
            case FLOAT:
                schema = Schema.create(Schema.Type.FLOAT);
                break;
            case DATE:
                schema = SpecificData.get().getConversionFor(LocalDateLogicalType.INSTANCE).getRecommendedSchema();
                break;
            case DATETIME:
                schema = SpecificData.get().getConversionFor(LocalDateTimeLogicalType.INSTANCE).getRecommendedSchema();
                break;
            case BOOLEAN:
                schema = Schema.create(Schema.Type.BOOLEAN);
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + columnType);
        }
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
}

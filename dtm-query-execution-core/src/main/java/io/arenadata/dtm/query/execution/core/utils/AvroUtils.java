package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.schema.codec.type.LocalDateLogicalType;
import io.arenadata.dtm.common.schema.codec.type.LocalDateTimeLogicalType;
import io.arenadata.dtm.common.schema.codec.type.LocalTimeLogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

public class AvroUtils {

    public static Schema.Field createSysOpField() {
        return new Schema.Field("sys_op", Schema.create(Schema.Type.INT), null, 0);
    }

    public static Schema.Field toSchemaField(EntityField column) {
        return column.getNullable() ? genNullableField(column) : genNonNullableField(column);
    }

    public static Schema metadataColumnTypeToAvroSchema(ColumnType columnType) {
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
            case TIME:
                schema = SpecificData.get().getConversionFor(LocalTimeLogicalType.INSTANCE).getRecommendedSchema();
                break;
            case TIMESTAMP:
                schema = SpecificData.get().getConversionFor(LocalDateTimeLogicalType.INSTANCE).getRecommendedSchema();
                break;
            case BOOLEAN:
                schema = Schema.create(Schema.Type.BOOLEAN);
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + columnType);
        }
        return schema;
    }

    private static Schema.Field genNullableField(EntityField column) {
        Schema.Field field = new Schema.Field(column.getName(),
                Schema.createUnion(Schema.create(Schema.Type.NULL), metadataColumnTypeToAvroSchema(column.getType())),
                null, Schema.Field.NULL_DEFAULT_VALUE);
        field.addProp("defaultValue", "null");
        return field;
    }

    private static Schema.Field genNonNullableField(EntityField column) {
        return new Schema.Field(column.getName(),
                metadataColumnTypeToAvroSchema(column.getType()),
                null);
    }
}

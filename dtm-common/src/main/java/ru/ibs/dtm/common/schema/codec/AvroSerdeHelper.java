package ru.ibs.dtm.common.schema.codec;

import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import ru.ibs.dtm.common.schema.codec.conversion.BigDecimalConversion;
import ru.ibs.dtm.common.schema.codec.conversion.LocalDateConversion;
import ru.ibs.dtm.common.schema.codec.conversion.LocalDateTimeConversion;
import ru.ibs.dtm.common.schema.codec.conversion.LocalTimeConversion;
import ru.ibs.dtm.common.schema.codec.type.BigDecimalLogicalType;
import ru.ibs.dtm.common.schema.codec.type.LocalDateLogicalType;
import ru.ibs.dtm.common.schema.codec.type.LocalDateTimeLogicalType;
import ru.ibs.dtm.common.schema.codec.type.LocalTimeLogicalType;

public abstract class AvroSerdeHelper {
    static {
        GenericData.get().addLogicalTypeConversion(LocalDateConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(BigDecimalConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalDateConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalTimeConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(BigDecimalConversion.getInstance());
        LogicalTypes.register(BigDecimalLogicalType.INSTANCE.getName(), schema -> BigDecimalLogicalType.INSTANCE);
        LogicalTypes.register(LocalDateTimeLogicalType.INSTANCE.getName(), schema -> LocalDateTimeLogicalType.INSTANCE);
        LogicalTypes.register(LocalDateLogicalType.INSTANCE.getName(), schema -> LocalDateLogicalType.INSTANCE);
        LogicalTypes.register(LocalTimeLogicalType.INSTANCE.getName(), schema -> LocalTimeLogicalType.INSTANCE);
    }
}

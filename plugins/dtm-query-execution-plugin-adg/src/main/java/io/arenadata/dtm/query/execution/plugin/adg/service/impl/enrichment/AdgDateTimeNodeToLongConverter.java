//package io.arenadata.dtm.query.execution.plugin.adg.service.impl.enrichment;
//
//import io.arenadata.dtm.common.model.ddl.ColumnType;
//import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.DateTimeNodeToLongConverter;
//import lombok.val;
//import org.apache.calcite.rex.RexCall;
//import org.apache.calcite.rex.RexNode;
//import org.springframework.stereotype.Component;
//
//import java.time.Instant;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeFormatterBuilder;
//import java.time.temporal.ChronoField;
//import java.time.temporal.ChronoUnit;
//
//@Component
//public class AdgDateTimeNodeToLongConverter implements DateTimeNodeToLongConverter {
//
//    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
//            .appendPattern("yyyy-MM-dd HH:mm:ss")
//            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
//            .toFormatter();
//    private static final ZoneId ZONE_ID = ZoneId.of("UTC");
//
//    @Override
//    public long convert(String stringValue, ColumnType type) {
////        val sqlType = valueOperand.getType().getSqlTypeName();
////        String stringValue = ((RexCall) valueOperand).getOperands().get(0).toString().replace("'", "");
//        switch (type) {
//            case TIME:
//                stringValue = "1970-01-01 " + stringValue;
//                return ChronoUnit.MICROS.between(Instant.EPOCH, LocalDateTime.parse(stringValue, TIMESTAMP_FORMATTER).atZone(ZONE_ID).toInstant());
//            case DATE:
//                return LocalDate.parse(stringValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
//            case TIMESTAMP:
//                return ChronoUnit.MICROS.between(Instant.EPOCH, LocalDateTime.parse(stringValue, TIMESTAMP_FORMATTER).atZone(ZONE_ID).toInstant());
//            default:
////                throw new RuntimeException("Cant convert");
//                return 0;
//        }
//    }
//}

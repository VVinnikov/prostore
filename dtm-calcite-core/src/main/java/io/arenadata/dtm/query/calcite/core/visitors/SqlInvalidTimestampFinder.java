package io.arenadata.dtm.query.calcite.core.visitors;

import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashSet;
import java.util.Set;

public class SqlInvalidTimestampFinder extends SqlBasicVisitor<Object> {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private static final String TIMESTAMP_TYPE_NAME = "TIMESTAMP";
    private final Set<String> invalidTimestamps = new HashSet<>();

    @Override
    public Object visit(SqlCall call) {
        if (SqlKind.CAST.equals(call.getKind())) {
            val sqlBasicCall = (SqlBasicCall) call;
            if (((SqlDataTypeSpec) sqlBasicCall.getOperands()[1]).getTypeName().getSimple().equals(TIMESTAMP_TYPE_NAME)) {
                val value = ((SqlCharStringLiteral) sqlBasicCall.getOperands()[0]).toValue();
                try {
                    TIMESTAMP_FORMATTER.parse(value);
                } catch (Exception e) {
                    invalidTimestamps.add(value);
                }
            }
        }
        return super.visit(call);
    }

    public Set<String> getInvalidTimestamps() {
        return invalidTimestamps;
    }
}

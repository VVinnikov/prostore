package io.arenadata.dtm.query.execution.plugin.adg.converter;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.ConverterConfiguration;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AdgTypeToSqlTypeConverterTest {

    private SqlTypeConverter typeConverter;
    private String charVal;
    private Integer intVal;
    private Long bigintVal;
    private Double doubleVal;
    private Float floatVal;
    private Long dateLongVal;
    private Long timeLongVal;
    private Long timestampLongVal;
    private Boolean booleanVal;
    private String uuidStrVal;
    private Map<String, Object> objMapVal;

    @BeforeEach
    void setUp() {
        typeConverter = new AdgTypeToSqlTypeConverter(new ConverterConfiguration().transformerMap());
        charVal = "111";
        intVal = 1;
        bigintVal = 100000000000L;
        doubleVal = 1.0d;
        floatVal = 1.0f;
        dateLongVal = 18540L;
        timeLongVal = 58742894000000L;
        timestampLongVal = 1601878742000L;
        booleanVal = true;
        uuidStrVal = "a7180dcb-b286-4168-a34a-eb378a69abd4";
        objMapVal = new HashMap<>();
        objMapVal.put("id", 1);
    }

    @Test
    void convert() {
        Map<ColumnType, Object> expectedValues = new HashMap<>();
        expectedValues.put(ColumnType.VARCHAR, charVal);
        expectedValues.put(ColumnType.CHAR, charVal);
        expectedValues.put(ColumnType.INT, intVal);
        expectedValues.put(ColumnType.BIGINT, bigintVal);
        expectedValues.put(ColumnType.DOUBLE, doubleVal);
        expectedValues.put(ColumnType.FLOAT, floatVal);
        expectedValues.put(ColumnType.DATE, Date.valueOf(LocalDate.ofEpochDay(dateLongVal)));
        expectedValues.put(ColumnType.TIME, Time.valueOf(LocalTime.ofNanoOfDay(timeLongVal)));
        expectedValues.put(ColumnType.TIMESTAMP, Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampLongVal), ZoneId.systemDefault())));
        expectedValues.put(ColumnType.BOOLEAN, booleanVal);
        expectedValues.put(ColumnType.UUID, UUID.fromString(uuidStrVal));
        expectedValues.put(ColumnType.ANY, JsonObject.mapFrom(objMapVal));

        assertAll("Varchar converting",
                () -> assertEquals(expectedValues.get(ColumnType.VARCHAR), typeConverter.convert(ColumnType.VARCHAR, charVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.VARCHAR, charVal) instanceof String)
        );
        assertAll("Char converting",
                () -> assertEquals(expectedValues.get(ColumnType.CHAR), typeConverter.convert(ColumnType.CHAR, charVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.CHAR, charVal) instanceof String)
        );
        assertAll("Int converting",
                () -> assertEquals(expectedValues.get(ColumnType.INT), typeConverter.convert(ColumnType.INT, intVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.INT, intVal) instanceof Integer)
        );
        assertAll("Bigint converting",
                () -> assertEquals(expectedValues.get(ColumnType.BIGINT), typeConverter.convert(ColumnType.BIGINT, bigintVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, bigintVal) instanceof Long)
        );
        assertAll("Bigint with small value converting",
                () -> assertEquals(1L, typeConverter.convert(ColumnType.BIGINT, intVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, intVal) instanceof Long)
        );
        assertAll("Double converting",
                () -> assertEquals(expectedValues.get(ColumnType.DOUBLE), typeConverter.convert(ColumnType.DOUBLE, doubleVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.DOUBLE, doubleVal) instanceof Double)
        );
        assertAll("Float converting",
                () -> assertEquals(expectedValues.get(ColumnType.FLOAT), typeConverter.convert(ColumnType.FLOAT, floatVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.FLOAT, floatVal) instanceof Float)
        );
        assertAll("Date converting",
                () -> assertEquals(expectedValues.get(ColumnType.DATE), typeConverter.convert(ColumnType.DATE, dateLongVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.DATE, dateLongVal) instanceof Date)
        );
        assertAll("Time converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIME), typeConverter.convert(ColumnType.TIME, timeLongVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.TIME, timeLongVal) instanceof Time)
        );
        assertAll("Timestamp converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIMESTAMP), typeConverter.convert(ColumnType.TIMESTAMP,
                        timestampLongVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.TIMESTAMP, timestampLongVal) instanceof Timestamp)
        );
        assertAll("Boolean converting",
                () -> assertEquals(expectedValues.get(ColumnType.BOOLEAN), typeConverter.convert(ColumnType.BOOLEAN, booleanVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BOOLEAN, booleanVal) instanceof Boolean)
        );
        assertAll("UUID converting",
                () -> assertEquals(expectedValues.get(ColumnType.UUID), typeConverter.convert(ColumnType.UUID, uuidStrVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.UUID, uuidStrVal) instanceof UUID)
        );
        assertAll("Any converting",
                () -> assertEquals(expectedValues.get(ColumnType.ANY), typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal))),
                () -> assertTrue(typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal)) instanceof JsonObject)
        );
    }

    @Test
    void convertWithNull() {
        charVal = null;
        intVal = null;
        bigintVal = null;
        doubleVal = null;
        floatVal = null;
        dateLongVal = null;
        timeLongVal = null;
        timestampLongVal = null;
        booleanVal = null;
        uuidStrVal = null;
        objMapVal = null;

        assertAll("Varchar converting",
                () -> assertNull(typeConverter.convert(ColumnType.VARCHAR, charVal))

        );
        assertAll("Char converting",
                () -> assertNull(typeConverter.convert(ColumnType.CHAR, charVal))
        );
        assertAll("Int converting",
                () -> assertNull(typeConverter.convert(ColumnType.INT, intVal))
        );
        assertAll("Bigint converting",
                () -> assertNull(typeConverter.convert(ColumnType.BIGINT, bigintVal))
        );
        assertAll("Double converting",
                () -> assertNull(typeConverter.convert(ColumnType.DOUBLE, doubleVal))
        );
        assertAll("Float converting",
                () -> assertNull(typeConverter.convert(ColumnType.FLOAT, floatVal))
        );
        assertAll("Date converting",
                () -> assertNull(typeConverter.convert(ColumnType.DATE, dateLongVal))
        );
        assertAll("Time converting",
                () -> assertNull(typeConverter.convert(ColumnType.TIME, timeLongVal))
        );
        assertAll("Timestamp converting",
                () -> assertNull(typeConverter.convert(ColumnType.TIMESTAMP, timestampLongVal))
        );
        assertAll("Boolean converting",
                () -> assertNull(typeConverter.convert(ColumnType.BOOLEAN, booleanVal))
        );
        assertAll("UUID converting",
                () -> assertNull(typeConverter.convert(ColumnType.UUID, uuidStrVal))
        );
        assertAll("Any converting",
                () -> assertNull(typeConverter.convert(ColumnType.ANY, objMapVal))
        );
    }
}

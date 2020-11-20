package io.arenadata.dtm.query.execution.plugin.adqm.converter;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.ConverterConfiguration;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class AdqmTypeToSqlTypeConverterTest {

    private static final ZoneId UTC_TIME_ZONE = ZoneId.of("UTC");
    private SqlTypeConverter typeConverter;
    private String charVal;
    private Long intVal;
    private Long bigintVal;
    private Double doubleVal;
    private Float floatVal;
    private Long dateLongVal;
    private Long timeLongVal;
    private Long timestampLongVal;
    private String timestampStrVal;
    private Boolean booleanVal;
    private String uuidStrVal;
    private BigInteger bigInteger;
    private Map<String, Object> objMapVal;

    @BeforeEach
    void setUp() {
        typeConverter = new AdqmTypeToSqlTypeConverter(new ConverterConfiguration().transformerMap(new DtmConfig() {
            @Override
            public ZoneId getTimeZone() {
                return UTC_TIME_ZONE;
            }
        }));
        charVal = "111";
        intVal = 1L;
        bigintVal = 1L;
        doubleVal = 1.0d;
        floatVal = 1.0f;
        dateLongVal = 18540L;
        timeLongVal = 58742894000000L;
        timestampLongVal = 1601878742000L;
        timestampStrVal = "2020-10-05 14:15:16.000000";
        booleanVal = true;
        uuidStrVal = "a7180dcb-b286-4168-a34a-eb378a69abd4";
        bigInteger = BigInteger.ONE;
        objMapVal = new HashMap<>();
        objMapVal.put("id", 1);
    }

    @Test
    void convert() {
        Map<ColumnType, Object> expectedValues = new HashMap<>();
        expectedValues.put(ColumnType.VARCHAR, charVal);
        expectedValues.put(ColumnType.CHAR, charVal);
        expectedValues.put(ColumnType.INT, intVal);
        expectedValues.put(ColumnType.BIGINT, Arrays.asList(bigintVal, bigInteger));
        expectedValues.put(ColumnType.DOUBLE, doubleVal);
        expectedValues.put(ColumnType.FLOAT, floatVal);
        expectedValues.put(ColumnType.DATE, Date.valueOf(LocalDate.ofEpochDay(dateLongVal)));
        expectedValues.put(ColumnType.TIME, timeLongVal / 1000);
        expectedValues.put(ColumnType.TIMESTAMP, Timestamp.from(LocalDateTime.parse(timestampStrVal,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")).atZone(UTC_TIME_ZONE).toInstant()));
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
                () -> assertTrue(typeConverter.convert(ColumnType.INT, intVal) instanceof Long)
        );
        assertAll("Bigint converting from long",
                () -> assertEquals(((List)expectedValues.get(ColumnType.BIGINT)).get(0),
                        typeConverter.convert(ColumnType.BIGINT, bigintVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, bigintVal) instanceof Long)
        );
        assertAll("Bigint converting from BigInteger",
                () -> assertEquals(((BigInteger)((List)expectedValues.get(ColumnType.BIGINT)).get(1)).longValue(),
                        typeConverter.convert(ColumnType.BIGINT, bigInteger)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, bigInteger) instanceof Long)
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
                () -> assertTrue(typeConverter.convert(ColumnType.TIME, timeLongVal) instanceof Number)
        );
        assertAll("Timestamp converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIMESTAMP), typeConverter.convert(ColumnType.TIMESTAMP,
                        timestampStrVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.TIMESTAMP, timestampStrVal) instanceof Timestamp)
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
        bigInteger = null;

        assertAll("Varchar converting",
                () -> assertNull(typeConverter.convert(ColumnType.VARCHAR, charVal))

        );
        assertAll("Char converting",
                () -> assertNull(typeConverter.convert(ColumnType.CHAR, charVal))
        );
        assertAll("Int converting",
                () -> assertNull(typeConverter.convert(ColumnType.INT, intVal))
        );
        assertAll("Bigint converting from long",
                () -> assertNull(typeConverter.convert(ColumnType.BIGINT, bigintVal))
        );
        assertAll("Bigint converting from bigInteger",
                () -> assertNull(typeConverter.convert(ColumnType.BIGINT, bigInteger))
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

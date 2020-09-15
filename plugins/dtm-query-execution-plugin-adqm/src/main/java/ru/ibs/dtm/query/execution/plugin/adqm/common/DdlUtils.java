package ru.ibs.dtm.query.execution.plugin.adqm.common;

import io.vertx.core.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.StringUtils;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DdlUtils {
    private DdlUtils() {}

    public final static String NULLABLE_FIELD = "%s Nullable(%s)";
    public final static String NOT_NULLABLE_FIELD = "%s %s";

    public static Optional<String> validateRequest(MppwRequest request) {
        if (request == null) {
            return Optional.of("MppwRequest should not be null");
        }

        QueryLoadParam loadParam = request.getQueryLoadParam();
        if (loadParam == null) {
            return Optional.of("MppwRequest.QueryLoadParam should not be null");
        }

        if (request.getSchema() == null) {
            return Optional.of("MppwRequest.schema should not be null");
        }

        return Optional.empty();
    }

    public static String getQualifiedTableName(@NonNull MppwRequest request,
                                               @NonNull AppConfiguration appConfiguration) {
        QueryLoadParam loadParam = request.getQueryLoadParam();

        String tableName = loadParam.getTableName();
        String schema = loadParam.getDatamart();
        String env = appConfiguration.getSystemName();
        return env + "__" + schema + "." + tableName;
    }

    public static Optional<Pair<String, String>> splitQualifiedTableName(@NonNull String table) {
        String[] parts = table.split("\\.");
        if (parts.length != 2) {
            return Optional.empty();
        }

        return Optional.of(Pair.of(parts[0], parts[1]));
    }

    public static String classTypeToNative(@NonNull ColumnType type) {
        switch (type) {
            case UUID: return "UUID";

            case ANY:
            case CHAR:
            case VARCHAR: return "String";

            case INT:
            case BIGINT:
            case DATE:
            case TIME: return "Int64";

            case BOOLEAN: return "UInt8";

            case FLOAT: return "Float32";
            case DOUBLE: return "Float64";

            case TIMESTAMP: return "DateTime64(6)";

            default: return "";
        }
    }

    public static String avroTypeToNative(@NonNull Schema f) {
        // we support UNION schema (with nullable option) and primitive type schemas
        switch (f.getType()) {
            case UNION:
                val fields = f.getTypes();
                val types = fields.stream().map(DdlUtils::avroTypeToNative).collect(Collectors.toList());
                if (types.size() == 2) { // We support only union (null, type)
                    int realTypeIdx = types.get(0).equalsIgnoreCase("NULL") ? 1 : 0;
                    return avroTypeToNative(fields.get(realTypeIdx));
                } else {
                    return "";
                }
            case STRING:
                return "String";
            case INT:
                return "Int32";
            case LONG:
                return "Int64";
            case FLOAT:
                return "Float32";
            case DOUBLE:
                return "Float64";
            case BOOLEAN:
                return "UInt8";
            case NULL:
                return "NULL";
            default:
                return "";
        }
    }

    public static String classFieldToString(@NonNull ClassField f) {
        String name = f.getName();
        String type = classTypeToNative(f.getType());
        String template = f.getNullable() ? NULLABLE_FIELD : NOT_NULLABLE_FIELD;

        return String.format(template, name, type);
    }

    public static String avroFieldToString(@NonNull Schema.Field f) {
        return avroFieldToString(f, true);
    }

    public static String avroFieldToString(@NonNull Schema.Field f, boolean isNullable) {
        String name = f.name();
        String type = avroTypeToNative(f.schema());
        String template = isNullable ? NULLABLE_FIELD : NOT_NULLABLE_FIELD;

        return String.format(template, name, type);
    }

    public static <T, E> Future<T> sequenceAll(@NonNull final List<E> actions,
                                               @NonNull final Function<E, Future<T>> action) {
        Future<T> result = null;
        for (E a: actions) {
            if (result == null) {
                result = action.apply(a);
            } else {
                result = result.compose(v -> action.apply(a));
            }
        }

        return result == null ? Future.succeededFuture() : result;
    }
}

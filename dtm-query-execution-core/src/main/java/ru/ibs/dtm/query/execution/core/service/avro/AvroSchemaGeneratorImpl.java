package ru.ibs.dtm.query.execution.core.service.avro;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.query.execution.core.utils.AvroUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AvroSchemaGeneratorImpl implements AvroSchemaGenerator {

    @Override
    public Schema generateTableSchema(Entity table, boolean withSysOpField) {
        List<Schema.Field> fields = getFields(table, withSysOpField);
        return Schema.createRecord(table.getName(), null, table.getSchema(), false, fields);
    }

    @NotNull
    private List<Schema.Field> getFields(Entity table, boolean withSysOpField) {
        val fields = table.getFields().stream()
            .sorted(Comparator.comparing(EntityField::getOrdinalPosition))
            .map(AvroUtils::toSchemaField)
            .collect(Collectors.toList());

        boolean hasAlreadySysOpField = table.getFields().stream()
                .anyMatch(f -> f.getName().equalsIgnoreCase("sys_op"));

        if (withSysOpField && !hasAlreadySysOpField) {
            fields.add(AvroUtils.createSysOpField());
        }

        return fields;
    }
}

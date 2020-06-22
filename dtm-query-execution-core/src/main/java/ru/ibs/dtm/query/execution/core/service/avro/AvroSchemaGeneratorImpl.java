package ru.ibs.dtm.query.execution.core.service.avro;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.utils.AvroUtils;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AvroSchemaGeneratorImpl implements AvroSchemaGenerator {

    @Override
    public Schema generateTableSchema(ClassTable table) {
        List<Schema.Field> fields = getFields(table);
        return Schema.createRecord(table.getName(), null, table.getSchema(), false, fields);
    }

    @NotNull
    private List<Schema.Field> getFields(ClassTable table) {
        val fields = table.getFields().stream()
                .map(AvroUtils::toSchemaField)
                .collect(Collectors.toList());
        fields.add(AvroUtils.createSysOpField());
        return fields;
    }
}

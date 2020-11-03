package io.arenadata.dtm.common.model.ddl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Physical model of the service database table
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class Entity implements Serializable, Cloneable {

    private static final String DEFAULT_SCHEMA = "test";

    private String name;
    private String schema;
    private String viewQuery;
    private EntityType entityType;
    private String externalTableFormat;
    private String externalTableSchema;
    private ExternalTableLocationType externalTableLocationType;
    private String externalTableLocationPath;
    private Integer externalTableDownloadChunkSize;
    private Integer externalTableUploadMessageLimit;
    private Set<SourceType> destination;
    private List<EntityField> fields;

    public Entity(String nameWithSchema, List<EntityField> fields) {
        this.fields = fields;
        parseNameWithSchema(nameWithSchema);
    }

    public Entity(String name, String schema, List<EntityField> fields) {
        this.name = name;
        this.schema = schema;
        this.fields = fields;
    }

    private void parseNameWithSchema(String nameWithSchema) {
        int indexComma = nameWithSchema.indexOf(".");
        this.schema = indexComma != -1 ? nameWithSchema.substring(0, indexComma) : DEFAULT_SCHEMA;
        this.name = nameWithSchema.substring(indexComma + 1);
    }

    @JsonIgnore
    public String getNameWithSchema() {
        return schema + "." + name;
    }

    @Override
    @SneakyThrows
    public Entity clone() {
        return (Entity) super.clone();
    }
}


package ru.ibs.dtm.common.model.ddl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Physical model of the service database table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Entity {

    private static final String DEFAULT_SCHEMA = "test";

    private String name;
    private String schema;
    private String viewQuery;
    private String externalTableFormat;
    private String externalTableSchema;
    private String externalTableLocationType;
    private String externalTableLocationPath;
    private Integer externalTableDownloadChunkSize;
    private Integer externalTableUploadMessageLimit;
    private List<String> destination;
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

}


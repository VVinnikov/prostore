package io.arenadata.dtm.query.execution.model.metadata;

import io.arenadata.dtm.common.model.ddl.Entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Schema Description SchemaDescription
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Datamart implements Serializable {
    /**
     * Schema name
     */
    private String mnemonic;
    /**
     * default showcase attribute
     */
    private Boolean isDefault = false;
    /**
     * Description of tables in the schema
     */
    private List<Entity> entities;
}



package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.model.ddl.Entity;

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




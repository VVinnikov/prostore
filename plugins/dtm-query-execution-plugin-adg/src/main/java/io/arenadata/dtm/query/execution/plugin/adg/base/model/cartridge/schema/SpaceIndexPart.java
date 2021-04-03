package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Part of composite index
 *
 * @path path
 * @type type
 * @isNullable is can be null
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpaceIndexPart {
    String path;
    String type;
    @JsonProperty("is_nullable")
    Boolean isNullable;
}

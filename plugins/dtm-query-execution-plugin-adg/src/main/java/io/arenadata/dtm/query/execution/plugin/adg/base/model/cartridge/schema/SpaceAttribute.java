package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Space attribute
 *
 * @isNullable is can be null
 * @name name
 * @type type
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpaceAttribute {
    @JsonProperty("is_nullable")
    Boolean isNullable;
    String name;
    SpaceAttributeTypes type;
}

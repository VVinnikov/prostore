package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Атрибут
 *
 * @isNullable принимает ли NULL
 * @name название
 * @type тип
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

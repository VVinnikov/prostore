package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Составной индекс
 *
 * @path указатель
 * @type тип
 * @isNullable принимает ли NULL
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

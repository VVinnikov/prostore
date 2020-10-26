package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Индекс
 *
 * @unique уникальность
 * @parts составной индекс
 * @parts тип
 * @parts название
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpaceIndex {
  Boolean unique;
  List<SpaceIndexPart> parts;
  SpaceIndexTypes type;
  String name;
}

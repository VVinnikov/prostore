package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Пространство
 *
 * @format список аттрибутов
 * @temporary временное
 * @engine движок
 * @isLocal локальное
 * @shardingKey ключ шарды
 * @indexes индексы
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Space {
  List<SpaceAttribute> format;
  Boolean temporary;
  SpaceEngines engine;
  @JsonProperty("is_local")
  Boolean isLocal;
  @JsonProperty("sharding_key")
  List<String> shardingKey;
  List<SpaceIndex> indexes;
}

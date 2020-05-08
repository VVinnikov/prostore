package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Движки пространства
 */
public enum SpaceEngines {
  MEMTX("memtx");

  private String name;

  SpaceEngines(String name) {
    this.name = name;
  }

  @JsonValue
  public String getName() {
    return name;
  }
}

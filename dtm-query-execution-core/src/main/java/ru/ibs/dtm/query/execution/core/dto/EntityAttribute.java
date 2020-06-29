package ru.ibs.dtm.query.execution.core.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityAttribute {
  @JsonIgnore
  private Integer id;
  private String mnemonic;
  private String dataType;
  private Integer length;
  private Integer accuracy;
  private String entityMnemonic;
  private String datamartMnemonic;

  public EntityAttribute(Integer id,
                         String mnemonic,
                         String dataType,
                         Integer length,
                         Integer accuracy,
                         String entityMnemonic,
                         String datamartMnemonic) {
    this.id = id;
    this.mnemonic = mnemonic;
    this.dataType = dataType;
    this.length = length;
    this.accuracy = accuracy;
    this.entityMnemonic = entityMnemonic;
    this.datamartMnemonic = datamartMnemonic;
  }
}

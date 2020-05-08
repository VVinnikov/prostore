package ru.ibs.dtm.query.execution.core.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

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

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getMnemonic() {
    return mnemonic;
  }

  public void setMnemonic(String mnemonic) {
    this.mnemonic = mnemonic;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public Integer getAccuracy() {
    return accuracy;
  }

  public void setAccuracy(Integer accuracy) {
    this.accuracy = accuracy;
  }

  public String getEntityMnemonic() {
    return entityMnemonic;
  }

  public void setEntityMnemonic(String entityMnemonic) {
    this.entityMnemonic = entityMnemonic;
  }

  public String getDatamartMnemonic() {
    return datamartMnemonic;
  }

  public void setDatamartMnemonic(String datamartMnemonic) {
    this.datamartMnemonic = datamartMnemonic;
  }
}

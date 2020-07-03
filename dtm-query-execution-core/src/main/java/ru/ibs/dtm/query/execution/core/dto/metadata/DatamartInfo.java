package ru.ibs.dtm.query.execution.core.dto.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatamartInfo {
  private Integer id;
  private String mnemonic;
  private List<DatamartEntity> entities;

  public DatamartInfo(Integer id, String mnemonic) {
    this.id = id;
    this.mnemonic = mnemonic;
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

  public List<DatamartEntity> getEntities() {
    return entities;
  }

  public void setEntities(List<DatamartEntity> entities) {
    this.entities = entities;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatamartInfo that = (DatamartInfo) o;
    return Objects.equals(id, that.id) &&
      Objects.equals(mnemonic, that.mnemonic) &&
      Objects.equals(entities, that.entities);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, mnemonic, entities);
  }
}

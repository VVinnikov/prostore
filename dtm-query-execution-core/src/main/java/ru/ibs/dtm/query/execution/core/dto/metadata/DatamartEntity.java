package ru.ibs.dtm.query.execution.core.dto.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatamartEntity {
  @JsonIgnore
  private Long id;
  private String mnemonic;
  private String datamartMnemonic;
  private List<EntityAttribute> attributes;

  public DatamartEntity(Long id, String mnemonic, String datamartMnemonic) {
    this.id = id;
    this.mnemonic = mnemonic;
    this.datamartMnemonic = datamartMnemonic;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getMnemonic() {
    return mnemonic;
  }

  public void setMnemonic(String mnemonic) {
    this.mnemonic = mnemonic;
  }

  public String getDatamartMnemonic() {
    return datamartMnemonic;
  }

  public void setDatamartMnemonic(String datamartMnemonic) {
    this.datamartMnemonic = datamartMnemonic;
  }

  public List<EntityAttribute> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<EntityAttribute> attributes) {
    this.attributes = attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatamartEntity that = (DatamartEntity) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}

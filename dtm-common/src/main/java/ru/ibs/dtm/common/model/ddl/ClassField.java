package ru.ibs.dtm.common.model.ddl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Физическая модель поля служебной БД
 */
public class ClassField {

  private final static Pattern nameWithSizePtn = Pattern.compile("\\w*(\\d)");

  private String name;
  private ClassTypes type;
  private Integer size;
  private Boolean isNull;
  private Boolean isPrimary;
  private String defaultValue;
  private String typeWithSize;

  public ClassField(String name, String typeWithSize, Boolean isNull, Boolean isPrimary, String defaultValue) {
    this.name = name;
    this.isNull = isNull;
    this.isPrimary = isPrimary;
    this.defaultValue = defaultValue;
    parseType(typeWithSize);
  }

  private void parseType(String typeWithSize) {
    Matcher matcher = nameWithSizePtn.matcher(typeWithSize);
    if (matcher.find()) {
      this.size = Integer.parseInt(typeWithSize.substring(matcher.start(), matcher.end()));
      this.type = ClassTypes.valueOf(typeWithSize.substring(0, matcher.start() - 1).toUpperCase());
    } else {
      this.type = ClassTypes.valueOf(typeWithSize.toUpperCase());
    }
    this.typeWithSize = typeWithSize;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ClassTypes getType() {
    return type;
  }

  public void setType(ClassTypes type) {
    this.type = type;
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public Boolean getNull() {
    return isNull;
  }

  public void setNull(Boolean aNull) {
    isNull = aNull;
  }

  public Boolean getPrimary() {
    return isPrimary;
  }

  public void setPrimary(Boolean primary) {
    isPrimary = primary;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public String getTypeWithSize() {
    return typeWithSize;
  }

  public void setTypeWithSize(String typeWithSize) {
    this.typeWithSize = typeWithSize;
  }
}

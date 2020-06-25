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
	private Integer primaryOrder;
	private Integer shardingOrder;
	private String defaultValue;
	private String typeWithSize;

	public ClassField(String name, String typeWithSize, Boolean isNull, Integer primaryOrder,
					  Integer shardingOrder, String defaultValue) {
		this.name = name;
		this.isNull = isNull;
		this.primaryOrder = primaryOrder;
		this.shardingOrder = shardingOrder;
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

	public Boolean isNullable() {
		return isNull;
	}
	public Boolean getNull() {
		return isNull;
	}

	public void setNull(Boolean aNull) {
		isNull = aNull;
	}

	public Integer getPrimaryOrder() {
		return primaryOrder;
	}

	public void setPrimaryOrder(Integer primaryOrder) {
		primaryOrder = primaryOrder;
	}

	public Integer getShardingOrder() {
		return shardingOrder;
	}

	public void setShardingOrder(Integer shardingOrder) {
		this.shardingOrder = shardingOrder;
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

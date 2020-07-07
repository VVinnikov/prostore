package ru.ibs.dtm.common.model.ddl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Физическая модель поля служебной БД
 */
@Data
@EqualsAndHashCode
public class ClassField {

	private final static Pattern nameWithSizePtn = Pattern.compile("\\w*(\\d)");

	private String name;
	private ClassTypes type;
	private Integer size;
    private Integer accuracy;
    private Boolean isPrimary;
	private Boolean nullable;
	private Integer primaryOrder;
	private Integer shardingOrder;
	private String defaultValue;
	private String typeWithSize;

	public ClassField(String name, String typeWithSize, Boolean nullable, Integer primaryOrder,
					  Integer shardingOrder, String defaultValue) {
		this.name = name;
		this.nullable = nullable;
		this.primaryOrder = primaryOrder;
		this.shardingOrder = shardingOrder;
		this.defaultValue = defaultValue;
		parseType(typeWithSize);
	}

    public ClassField(String name, ClassTypes type, Boolean isNull, Boolean isPrimary) {
        this.name = name;
        this.type = type;
        this.nullable = isNull;
        this.isPrimary = isPrimary;
    }

    public ClassField(String name, String typeWithSize, Boolean nullable, Boolean isPrimary, String defaultValue) {
        this.name = name;
        this.nullable = nullable;
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

}

package ru.ibs.dtm.common.model.ddl;

import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ClassFieldUtils {

	public static List<ClassField> getPrimaryKeyList(final List<ClassField> fields) {
		return fields.stream()
				.filter(f -> f.getPrimaryOrder() != null)
				.sorted(Comparator.comparing(ClassField::getPrimaryOrder))
				.collect(toList());
	}

	public static List<ClassField> getShardingKeyList(final List<ClassField> fields) {
		return fields.stream()
				.filter(f -> f.getShardingOrder() != null)
				.sorted(Comparator.comparing(ClassField::getShardingOrder))
				.collect(toList());
	}

}

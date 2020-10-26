package io.arenadata.dtm.common.model.ddl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class EntityFieldUtils {

	private static final List<String> pkSystemField = Arrays.asList("sys_from");

	public static List<EntityField> getPrimaryKeyList(final List<EntityField> fields) {
		return fields.stream()
				.filter(f -> f.getPrimaryOrder() != null)
				.sorted(Comparator.comparing(EntityField::getPrimaryOrder))
				.collect(toList());
	}

	public static List<EntityField> getPrimaryKeyListWithSysFields(final List<EntityField> fields) {
		return fields.stream()
				.filter(f -> f.getPrimaryOrder() != null || isSystemFieldForPk(f.getName()))
				.sorted(Comparator.comparing(EntityField::getPrimaryOrder))
				.collect(toList());
	}

	public static List<EntityField> getShardingKeyList(final List<EntityField> fields) {
		return fields.stream()
				.filter(f -> f.getShardingOrder() != null)
				.sorted(Comparator.comparing(EntityField::getShardingOrder))
				.collect(toList());
	}

	private static boolean isSystemFieldForPk(final String fieldName) {
		return pkSystemField.contains(fieldName);
	}

}

package ru.ibs.dtm.query.execution.core.utils;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.util.StringUtils;

/**
 * Генератор наименований Бинов.
 * Для именованных бинов оставляет их имя,
 * для всех остальных в качестве имени задает полное имя класса
 */
public class BeanNameGenerator extends AnnotationBeanNameGenerator {

  @Override
  public String generateBeanName(BeanDefinition definition, BeanDefinitionRegistry registry) {
    return (definition instanceof AnnotatedBeanDefinition &&
      StringUtils.hasText(determineBeanNameFromAnnotation((AnnotatedBeanDefinition) definition))) ?
      super.generateBeanName(definition, registry) : definition.getBeanClassName();
  }
}

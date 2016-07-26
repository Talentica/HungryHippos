package com.talentica.hungryHippos.utility;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * @author bramp
 */
public class SubFieldTestClass extends FieldTestClass {
  Object subField = new Object();

  public Set<Object> allFields() {
    return ImmutableSet.builder()
        .addAll(super.allFields())
        .add(subField).build();
  }
}

/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class ObjectGraph {
  final Map<Object, Class> visited = new IdentityHashMap<Object, Class>();
  final Queue<Object> toVisit = new ArrayDeque<Object>();

  final Visitor visitor;

  boolean excludeTransient = true;
  boolean excludeStatic = true;

  public interface Visitor {
    boolean visit(Object object, Class clazz);
  }

  ObjectGraph(Visitor visitor) {
    this.visitor = visitor;
  }

  static public ObjectGraph visitor(Visitor visitor) {
    return new ObjectGraph(visitor);
  }

  public ObjectGraph includeTransient() {
    excludeTransient = false;
    return this;
  }

  public ObjectGraph excludeTransient() {
    excludeTransient = true;
    return this;
  }

  public ObjectGraph includeStatic() {
    excludeStatic = false;
    return this;
  }

  public ObjectGraph excludeStatic() {
    excludeStatic = true;
    return this;
  }

  public void traverse(Object root) {
    // Reset the state
    visited.clear();
    toVisit.clear();

    if (root == null)
      return;

    addIfNotVisited(root, root.getClass());
    start();
  }


  private boolean canDescend(Class clazz) {
    return !clazz.isPrimitive();
  }

  private void addIfNotVisited(Object object, Class clazz) {
    if (object != null && !visited.containsKey(object)) {
      toVisit.add(object);
      visited.put(object, clazz);
    }
  }


  private List<Field> getAllFields(List<Field> fields, Class clazz) {
    fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

    if (clazz.getSuperclass() != null) {
      getAllFields(fields, clazz.getSuperclass());
    }

    return Collections.unmodifiableList(fields);
  }

  private void start() {

    while (!toVisit.isEmpty()) {

      Object obj = toVisit.remove();
      Class clazz = visited.get(obj);

      boolean terminate = visitor.visit(obj, clazz);
      if (terminate)
        return;

      if (!canDescend(clazz))
        continue;

      if (clazz.isArray()) {
        Class arrayType = clazz.getComponentType();

        final int len = Array.getLength(obj);

        for (int i = 0; i < len; i++) {
          addIfNotVisited(Array.get(obj, i), arrayType);
        }

      } else {
        List<Field> fields = getAllFields(new ArrayList<Field>(), obj.getClass());
        for (Field field : fields) {
          int modifiers = field.getModifiers();

          if (excludeStatic && (modifiers & Modifier.STATIC) == Modifier.STATIC)
            continue;

          if (excludeTransient && (modifiers & Modifier.TRANSIENT) == Modifier.TRANSIENT)
            continue;

          try {
            field.setAccessible(true);
            Object value = field.get(obj);
            addIfNotVisited(value, field.getType());

          } catch (IllegalAccessException e) {
          }
        }
      }
    }
  }
}

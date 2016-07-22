package com.talentica.hungryHippos.coordination.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to make the any field in the Class as transient for the zookeeper node as to
 * whether to create node or not.
 * 
 * @author pooshans
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ZkTransient {
  boolean value() default true;
}

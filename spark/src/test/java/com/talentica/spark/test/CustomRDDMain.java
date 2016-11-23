/**
 * 
 */
package com.talentica.spark.test;

import java.nio.ByteBuffer;

/**
 * @author pooshans
 * @param <T>
 *
 */
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

public class CustomRDDMain {
  private static final ClassTag<ByteBuffer> BYTE_TAG =
      ClassManifestFactory$.MODULE$.fromClass(ByteBuffer.class);

  public static void main(final String[] args) {
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("CustomRDDApp");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      System.out.println(new AlphabetRDD(sc.sc()).toJavaRDD().collect());
    }
  }

  public static class AlphabetRDD extends RDD<ByteBuffer> {
    private static final long serialVersionUID = 1L;

    public AlphabetRDD(SparkContext sc) {
      super(sc, new ArrayBuffer<Dependency<?>>(), BYTE_TAG);
    }

    @Override
    public Iterator<ByteBuffer> compute(Partition partition, TaskContext taskContext) {
      AlphabetRangePartition p = (AlphabetRangePartition) partition;
      return new CharacterIterator(p.from, p.to);
    }

    @Override
    public Partition[] getPartitions() {
      return new Partition[] {new AlphabetRangePartition(0, 'A', 'K'),
          new AlphabetRangePartition(1, 'T', 'Z')};
    }

  }

  /**
   * A partition representing letters of the Alphabet between a range
   */
  public static class AlphabetRangePartition implements Partition {
    private static final long serialVersionUID = 1L;
    private int index;
    private char from;
    private char to;

    public AlphabetRangePartition(int index, char c, char d) {
      this.index = index;
      this.from = c;
      this.to = d;
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public int hashCode() {
      return index();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof AlphabetRangePartition)) {
        return false;
      }
      return ((AlphabetRangePartition) obj).index != index;
    }
  }

  /**
   * Iterators over all characters between two characters
   */
  public static class CharacterIterator extends AbstractIterator<ByteBuffer> {
    private char next;
    private char last;

    public CharacterIterator(char from, char to) {
      next = from;
      this.last = to;
    }

    @Override
    public boolean hasNext() {
      return next <= last;
    }

    @Override
    public ByteBuffer next() {
      //return Character.toString(next++);
      return ByteBuffer.allocate(0);
    }
  }

}

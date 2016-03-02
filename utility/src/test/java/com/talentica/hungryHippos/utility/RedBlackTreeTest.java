package com.talentica.hungryHippos.utility;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.ValueSet;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectRBTreeMap;

public class RedBlackTreeTest {

	private RedBlackTree<Integer, String> redBlackTree = null;

	@Before
	public void setup() {
		redBlackTree = new RedBlackTree<>();
	}

	@Test
	public void testInsert() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(1, "One");
		redBlackTree.insert(12, "Twelve");
		Assert.assertEquals(2, redBlackTree.size());
	}

	@Test
	public void testSearch1() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(1, "One");
		redBlackTree.insert(12, "Twelve");
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.search(12);
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(12), redBlackNode.getKey());
		Assert.assertEquals("Twelve", redBlackNode.getValue());
	}

	@Test
	public void testSearch2() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(5, "Five");
		redBlackTree.insert(15, "Fifteen");
		redBlackTree.insert(20, "Twenty");
		redBlackTree.insert(2, "Two");
		redBlackTree.insert(1, "One");
		redBlackTree.insert(9, "Nine");
		redBlackTree.insert(6, "Six");
		redBlackTree.insert(28, "Twenty Eight");
		redBlackTree.insert(99, "Ninety Nine");
		redBlackTree.insert(55, "Fifty Five");
		redBlackTree.insert(22, "Twenty Two");
		redBlackTree.insert(21, "Twenty One");
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(99), redBlackNode.getKey());
		Assert.assertEquals("Ninety Nine", redBlackNode.getValue());
	}

	@Test
	public void testSearchMaximum1() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99, "Ninety Nine");
		redBlackTree.insert(95, "Ninety Five");
		redBlackTree.insert(94, "Ninety Founr");
		redBlackTree.insert(91, "Ninety One");
		redBlackTree.insert(93, "Ninety Three");
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(99), redBlackNode.getKey());
	}

	@Test
	public void testRemove() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99, "Ninety Nine");
		redBlackTree.insert(95, "Ninety Five");
		redBlackTree.insert(94, "Ninety Four");
		redBlackTree.insert(91, "Ninety One");
		redBlackTree.insert(93, "Ninety Three");
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.search(93);
		redBlackTree.remove(redBlackNode);
		redBlackNode = redBlackTree.search(93);
		Assert.assertNull(redBlackNode);
	}

	@Test
	public void testRemoveMaximum() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99, "Ninety Nine");
		redBlackTree.insert(95, "Ninety Five");
		redBlackTree.insert(94, "Ninety Four");
		redBlackTree.insert(91, "Ninety One");
		redBlackTree.insert(93, "Ninety Three");
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.search(99);
		Assert.assertNotNull(redBlackNode);
		redBlackTree.removeMaximum();
		redBlackNode = redBlackTree.search(99);
		Assert.assertNull(redBlackNode);
		Assert.assertEquals(4, redBlackTree.size());
		redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(95), redBlackNode.getKey());
	}

	@Test
	public void testRemoveMaximumAfterNewInserts() {
		testRemoveMaximum();
		redBlackTree.insert(96, "Ninety Six");
		Assert.assertEquals(5, redBlackTree.size());
		RedBlackNode<Integer, String> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(96), redBlackNode.getKey());
	}

	@Test
	public void testSearchByValueSetKey() {
		RedBlackTree<ValueSet, String> redBlackTree = new RedBlackTree<>();
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new Integer[] { 10, 12 });
		redBlackTree.insert(valueSet1, "One");
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new Integer[] { 10, 13 });
		redBlackTree.insert(valueSet2, "Two");
		ValueSet valueSet3 = new ValueSet(new int[] { 0, 1 }, new Integer[] { 10, 14 });
		redBlackTree.insert(valueSet3, "Three");
		RedBlackNode<ValueSet, String> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		ValueSet maxValueSetExpected = new ValueSet(new int[] { 0, 1 }, new Integer[] { 10, 14 });
		Assert.assertEquals(maxValueSetExpected, redBlackNode.getKey());
		Assert.assertEquals("Three", redBlackNode.getValue());
	}

	@Test
	public void testIterator() {
		redBlackTree.insert(5, "Five");
		redBlackTree.insert(15, "Fifteen");
		redBlackTree.insert(20, "Twenty");
		redBlackTree.insert(2, "Two");
		redBlackTree.insert(1, "One");
		redBlackTree.insert(9, "Nine");
		redBlackTree.insert(6, "Six");
		redBlackTree.insert(28, "Twenty Eight");
		redBlackTree.insert(99, "Ninety Nine");
		redBlackTree.insert(55, "Fifty Five");
		redBlackTree.insert(22, "Twenty Two");
		redBlackTree.insert(21, "Twenty One");
		Iterator<RedBlackNode<Integer, String>> iterator = redBlackTree.iterator();
		Assert.assertNotNull(iterator);
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(1, iterator.next());
	}

	private static final int noOfItemsToPut = 10000000;

	private static long startTime = System.currentTimeMillis();

	private static long endTime = System.currentTimeMillis();

	public static void main(String[] args) {
		// testPerformanceOfRedBlackTree();
		// testPerformanceOfObj2ObjHashMap();
		// testPerformanceOfObject2RBTreeMap();
		// testPerformanceOfJavaHashMap();
		testPerformanceOfJavaTreeMap();
	}

	private static void testPerformanceOfObject2RBTreeMap() {
		long freeMemoryAtStart = MemoryStatus.getFreeMemory();
		startTime = System.currentTimeMillis();
		Object2ObjectRBTreeMap<ValueSet, Integer> fastutilObj2ObjRBTreeMap = new Object2ObjectRBTreeMap<>();
		for (int i = 0, j = 0; i < noOfItemsToPut; i++, j++) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastutilObj2ObjRBTreeMap.put(valueSet, i);
		}
		endTime = System.currentTimeMillis();
		System.out.println("It took " + (endTime - startTime) + " ms to put " + noOfItemsToPut
				+ " items into Object2ObjectRBTreeMap");

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastutilObj2ObjRBTreeMap.get(valueSet);
		}
		endTime = System.currentTimeMillis();
		long freeMemoryAtEnd = MemoryStatus.getFreeMemory();
		System.out.println("It took " + (endTime - startTime) + " ms to get " + noOfItemsToPut
				+ " items from Object2ObjectRBTreeMap");
		System.out.println(
				"Approx. memory consumed in MBs is(Object2ObjectRBTreeMap):" + (freeMemoryAtEnd - freeMemoryAtStart));

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastutilObj2ObjRBTreeMap.remove(valueSet);
		}
		endTime = System.currentTimeMillis();
		System.out.println("It took " + (endTime - startTime) + " ms to remove " + noOfItemsToPut
				+ " items from Object2ObjectRBTreeMap");
	}

	private static void testPerformanceOfObj2ObjHashMap() {
		long freeMemoryAtStart = MemoryStatus.getFreeMemory();
		startTime = System.currentTimeMillis();
		Object2ObjectOpenHashMap<ValueSet, Integer> fastObj2ObjOpenHashMap = new Object2ObjectOpenHashMap<>();
		for (int i = 0, j = 0; i < noOfItemsToPut; i++, j++) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastObj2ObjOpenHashMap.put(valueSet, i);
		}
		endTime = System.currentTimeMillis();
		System.out.println("It took " + (endTime - startTime) + " ms to put " + noOfItemsToPut
				+ " items into Object2ObjectOpenHashMap");

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastObj2ObjOpenHashMap.get(valueSet);
		}
		endTime = System.currentTimeMillis();
		long freeMemoryAtEnd = MemoryStatus.getFreeMemory();
		System.out.println("It took " + (endTime - startTime) + " ms to get " + noOfItemsToPut
				+ " items from Object2ObjectOpenHashMap");
		System.out.println("Approx. memory consumed in MBs is(Object2ObjectOpenHashMap): "
				+ (freeMemoryAtEnd - freeMemoryAtStart));

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			fastObj2ObjOpenHashMap.remove(valueSet);
		}
		endTime = System.currentTimeMillis();
		System.out.println("It took " + (endTime - startTime) + " ms to remove " + noOfItemsToPut
				+ " items from Object2ObjectOpenHashMap");
	}

	private static void testPerformanceOfRedBlackTree() {
		long freeMemoryAtStart = MemoryStatus.getFreeMemory();
		startTime = System.currentTimeMillis();
		RedBlackTree<ValueSet, Integer> redBlackTree = new RedBlackTree<>();

		for (int i = 0, j = 0; i < noOfItemsToPut; i++, j++) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			redBlackTree.insert(valueSet, i);
		}

		endTime = System.currentTimeMillis();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to put " + noOfItemsToPut + " items into redblack tree");

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			redBlackTree.search(valueSet);
		}
		endTime = System.currentTimeMillis();
		long freeMemoryAtEnd = MemoryStatus.getFreeMemory();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to get " + noOfItemsToPut + " items from redblack tree");
		System.out
				.println("Approx. memory consumed in MBs is(redblack tree): " + (freeMemoryAtEnd - freeMemoryAtStart));

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1; i >= 0; i--) {
			redBlackTree.removeMaximum();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Size ofd red black tree after removing elements is: " + (redBlackTree.size() - 1));
		System.out.println(
				"It took " + (endTime - startTime) + " ms to remove " + noOfItemsToPut + " items from redblack tree");
	}

	private static void testPerformanceOfJavaTreeMap() {
		long freeMemoryAtStart = MemoryStatus.getFreeMemory();
		startTime = System.currentTimeMillis();
		TreeMap<ValueSet, Integer> javaTreeMap = new TreeMap<>();

		for (int i = 0, j = 0; i < noOfItemsToPut; i++, j++) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaTreeMap.put(valueSet, i);
		}

		endTime = System.currentTimeMillis();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to put " + noOfItemsToPut + " items into java treemap");

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaTreeMap.get(valueSet);
		}
		endTime = System.currentTimeMillis();
		long freeMemoryAtEnd = MemoryStatus.getFreeMemory();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to get " + noOfItemsToPut + " items from java treemap");
		System.out.println("Approx. memory consumed in MBs is(java treemap): " + (freeMemoryAtEnd - freeMemoryAtStart));

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaTreeMap.remove(valueSet);
		}
		endTime = System.currentTimeMillis();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to remove " + noOfItemsToPut + " items from java treemap");
	}

	private static void testPerformanceOfJavaHashMap() {
		long freeMemoryAtStart = MemoryStatus.getFreeMemory();
		startTime = System.currentTimeMillis();
		HashMap<ValueSet, Integer> javaHashMap = new HashMap<>();

		for (int i = 0, j = 0; i < noOfItemsToPut; i++, j++) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaHashMap.put(valueSet, i);
		}

		endTime = System.currentTimeMillis();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to put " + noOfItemsToPut + " items into java hashmap");

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaHashMap.get(valueSet);
		}
		endTime = System.currentTimeMillis();
		long freeMemoryAtEnd = MemoryStatus.getFreeMemory();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to get " + noOfItemsToPut + " items from java hashmap");
		System.out.println("Approx. memory consumed in MBs is(java hashmap): " + (freeMemoryAtEnd - freeMemoryAtStart));

		startTime = System.currentTimeMillis();
		for (int i = noOfItemsToPut - 1, j = noOfItemsToPut - 1; i >= 0; i--, j--) {
			ValueSet valueSet = new ValueSet(new int[] { i, j }, new Integer[] { i, j });
			javaHashMap.remove(valueSet);
		}
		endTime = System.currentTimeMillis();
		System.out.println(
				"It took " + (endTime - startTime) + " ms to remove " + noOfItemsToPut + " items from java hashmap");
	}

}

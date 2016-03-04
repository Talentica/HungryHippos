package com.talentica.hungryHippos.utility;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.ValueSet;

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

}

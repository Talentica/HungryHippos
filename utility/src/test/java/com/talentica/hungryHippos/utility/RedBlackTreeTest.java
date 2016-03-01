package com.talentica.hungryHippos.utility;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class RedBlackTreeTest {

	private RedBlackTree<Integer> redBlackTree = null;

	@Before
	public void setup(){
		redBlackTree = new RedBlackTree<>();
	}

	@Test
	public void testInsert() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(1);
		redBlackTree.insert(12);
		Assert.assertEquals(2, redBlackTree.size());
	}
	
	@Test
	public void testSearch() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(1);
		redBlackTree.insert(12);
		RedBlackNode<Integer> redBlackNode = redBlackTree.search(12);
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(12), redBlackNode.key);
	}

	@Test
	public void testSearch2() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(5);
		redBlackTree.insert(15);
		redBlackTree.insert(20);
		redBlackTree.insert(2);
		redBlackTree.insert(1);
		redBlackTree.insert(9);
		redBlackTree.insert(6);
		redBlackTree.insert(28);
		redBlackTree.insert(99);
		redBlackTree.insert(55);
		redBlackTree.insert(21);
		redBlackTree.insert(21);
		RedBlackNode<Integer> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(99), redBlackNode.key);
	}

	@Test
	public void testSearchMaximum1() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99);
		redBlackTree.insert(95);
		redBlackTree.insert(94);
		redBlackTree.insert(91);
		redBlackTree.insert(93);
		RedBlackNode<Integer> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(99), redBlackNode.key);
	}

	@Test
	public void testRemove() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99);
		redBlackTree.insert(95);
		redBlackTree.insert(94);
		redBlackTree.insert(91);
		redBlackTree.insert(93);
		RedBlackNode<Integer> redBlackNode = redBlackTree.search(93);
		redBlackTree.remove(redBlackNode);
		redBlackNode = redBlackTree.search(93);
		Assert.assertNull(redBlackNode);
	}

	@Test
	public void testRemoveMaximum() {
		Assert.assertEquals(1, redBlackTree.size());
		redBlackTree.insert(99);
		redBlackTree.insert(95);
		redBlackTree.insert(94);
		redBlackTree.insert(91);
		redBlackTree.insert(93);
		RedBlackNode<Integer> redBlackNode = redBlackTree.search(99);
		Assert.assertNotNull(redBlackNode);
		redBlackTree.removeMaximum();
		redBlackNode = redBlackTree.search(99);
		Assert.assertNull(redBlackNode);
		Assert.assertEquals(4, redBlackTree.size());
		redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(95), redBlackNode.key);
	}

	@Test
	public void testRemoveMaximumAfterNewInserts() {
		testRemoveMaximum();
		redBlackTree.insert(96);
		Assert.assertEquals(5, redBlackTree.size());
		RedBlackNode<Integer> redBlackNode = redBlackTree.searchMaximum();
		Assert.assertNotNull(redBlackNode);
		Assert.assertEquals(Integer.valueOf(96), redBlackNode.key);
	}

}

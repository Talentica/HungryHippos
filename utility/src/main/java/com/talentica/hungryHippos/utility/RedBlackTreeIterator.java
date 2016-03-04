package com.talentica.hungryHippos.utility;

import java.util.Iterator;

public class RedBlackTreeIterator<T extends Comparable<T>, V> implements Iterator<RedBlackNode<T, V>> {

	private RedBlackTree<T, V> redBlackTree;

	private RedBlackNode<T, V> currentNode;

	public RedBlackTreeIterator(RedBlackTree<T, V> redBlackTreeParam) {
		this.redBlackTree = redBlackTreeParam;
		currentNode = redBlackTree.getRoot();
	}

	@Override
	public boolean hasNext() {
		if (currentNode != null || currentNode.numLeft > 0 || currentNode.numRight > 0) {
			return true;
		}
		return false;
	}

	@Override
	public RedBlackNode<T, V> next() {
		RedBlackNode<T, V> currentIteration = null;
		while (currentIteration != null) {
			currentIteration = currentIteration.left;
		}
		return null;
	}

}
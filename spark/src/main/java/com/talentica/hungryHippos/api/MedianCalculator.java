package com.talentica.hungryHippos.api;

/**
 * @author pooshans
 *
 */
public class MedianCalculator {

	private Node root;
	private int[] keys;

	public MedianCalculator(int... keys) {
		if (keys == null || keys.length == 0) {
			throw new IllegalArgumentException("Null or empty array");
		}
		this.keys = keys;
		insert(keys);
	}

	public double getMedian() {
		return traverseTree(root, false);
	}

	private double traverseTree(Node n, boolean flag) {
		if (n == null) {
			return 0.0;
		} else {

			int leftKeyCount = childKeyCount(n.left);
			int rightKeyCount = childKeyCount(n.right);

			if (leftKeyCount == 0 && rightKeyCount == 0) {
				if (keys.length % 2 == 0 && !flag) {
					if (n.keyCount == 1) {
						/* no duplicate at current node */
						return (n.key + n.parent.key) / 2.0;
					} else {
						/*
						 * mid point found at current node and it's duplicate
						 * value, simply return it.
						 */
						return n.key;
					}
				} else {
					return n.key;
				}
			}

			if ((n.keyCount + leftKeyCount + n.leftCarry) < (rightKeyCount + n.rightCarry)) {
				if (n.right != null) {
					n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
					n.right.parent = n;
				} else {
					if (n.left != null) {
						return (n.key + n.left.key) / 2.0;
					}
				}
				return traverseTree(n.right, flag);
			} else if ((n.keyCount + rightKeyCount + n.rightCarry) < (leftKeyCount + n.leftCarry)) {
				if (n.left != null) {
					n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
					n.left.parent = n;
				} else {
					if (n.right != null) {
						return (n.key + n.right.key) / 2.0;
					}
				}
				return traverseTree(n.left, flag);
			} else if ((n.keyCount + leftKeyCount + n.leftCarry) == (rightKeyCount + n.rightCarry)) {
				if (n.right != null) {
					n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
					n.right.parent = n;
				} else {
					if (n.left != null) {
						return (n.key + n.left.key) / 2.0;
					}
				}
				return (n.key + traverseTree(n.right, true)) / 2.0;
			} else if ((n.keyCount + rightKeyCount + n.rightCarry) == (leftKeyCount + n.leftCarry)) {
				if (n.left != null) {
					n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
					n.left.parent = n;
				} else {
					if (n.right != null) {
						return (n.key + n.right.key) / 2.0;
					}
				}
				return (n.key + traverseTree(n.left, true)) / 2.0;
			} else {
				return n.key;
			}
		}
	}

	private int childKeyCount(Node n) {
		if (n == null) {
			return 0;
		} else {
			int leftKeyCount = childKeyCount(n.left);
			int rightKeyCount = childKeyCount(n.right);
			return leftKeyCount + rightKeyCount + n.keyCount;
		}
	}

	private Node insert(Node parent, int key) {
		if (parent == null) {
			return new Node(key);
		}
		boolean balancingNeeded = false;
		if (key < parent.key) {
			parent.left = insert(parent.left, key);
			balancingNeeded = true;
		} else if (key > parent.key) {
			parent.right = insert(parent.right, key);
			balancingNeeded = true;
		} else {
			parent.keyCount++;
			balancingNeeded = false;
		}
		return balancingNeeded ? balance(parent) : parent;
	}

	private Node balance(Node p) {
		fixHeightAndChildCount(p);
		if (bfactor(p) == 2) {
			if (bfactor(p.right) < 0) {
				p.right = rotateRight(p.right);
			}
			return rotateLeft(p);
		}
		if (bfactor(p) == -2) {
			if (bfactor(p.left) > 0) {
				p.left = rotateLeft(p.left);
			}
			return rotateRight(p);
		}
		return p;
	}

	private Node rotateRight(Node p) {
		Node q = p.left;
		p.left = q.right;
		q.right = p;
		fixHeightAndChildCount(p);
		fixHeightAndChildCount(q);
		return q;
	}

	private Node rotateLeft(Node q) {
		Node p = q.right;
		q.right = p.left;
		p.left = q;
		fixHeightAndChildCount(q);
		fixHeightAndChildCount(p);
		return p;
	}

	private void fixHeightAndChildCount(Node p) {
		int hl = height(p.left);
		int hr = height(p.right);
		p.height = (hl > hr ? hl : hr) + 1;
		p.childCount = 0;
		if (p.left != null) {
			p.childCount = p.left.childCount + 1;
		}
		if (p.right != null) {
			p.childCount += p.right.childCount + 1;
		}
	}

	private int height(Node p) {
		return p == null ? 0 : p.height;
	}

	private int bfactor(Node p) {
		return height(p.right) - height(p.left);
	}

	public void insert(int... keys) {
		for (int key : keys) {
			root = insert(root, key);
		}
	}

	private static class Node {

		private Node left;
		private Node right;
		private final int key;
		private int height;
		private int childCount;
		private int keyCount = 0;
		private int leftCarry = 0;
		private int rightCarry = 0;
		private Node parent;

		private Node(int value) {
			key = value;
			height = 1;
			keyCount++;
		}

		@Override
		public String toString() {
			return Integer.toString(key);
		}
	}

}

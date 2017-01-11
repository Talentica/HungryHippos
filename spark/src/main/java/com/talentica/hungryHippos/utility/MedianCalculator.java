package com.talentica.hungryHippos.utility;

/**
 * This is the median calculator using AVL tree.
 * 
 * @author pooshans
 *
 */
public class MedianCalculator<T extends Comparable<? super T>> {

	private Node root;
	private int totalCount;
	private final static double denominator = 2.0;

	public MedianCalculator() {
	}

	public MedianCalculator(T... keys) {
		if (keys == null || keys.length == 0) {
			throw new IllegalArgumentException("Null or empty array");
		}
		this.totalCount = keys.length;
		insert(keys);
	}

	/**
	 * @return Returns the median.
	 */
	public double getMedian() {
		return traverseTree(root, false);
	}

	/**
	 * @param key
	 *            : Key is sequentially added to the AVL tree.
	 */
	public void add(T key) {
		root = insert(root, key);
		totalCount++;
	}

	/**
	 * @param n
	 *            : It is the root of the AVL tree form where the actual
	 *            traversal start for median search.
	 * @param midPointFound
	 *            : It the identifier which signal that one key is found in mid
	 *            point and start looking for other one.
	 * @return It returns the median.
	 */
	private double traverseTree(Node n, boolean midPointFound) {
		if (n == null) {
			return 0.0;
		} else {

			int leftKeyCount = childKeyCount(n.left);
			int rightKeyCount = childKeyCount(n.right);

			if (leftKeyCount == 0 && rightKeyCount == 0) {
				if (totalCount % 2 == 0 && !midPointFound) {
					if (n.keyCount == 1) {
						/* no duplicate at current node */
						if (n.key instanceof Integer) {
							return (Integer.valueOf(n.key.toString()) + Integer.valueOf(n.parent.key.toString()))
									/ denominator;
						} else if (n.key instanceof Double) {
							return (Double.valueOf(n.key.toString()) + Double.valueOf(n.parent.key.toString()))
									/ denominator;
						}
					} else {
						/*
						 * mid point found at current node and it's duplicate
						 * value, simply return it.
						 */
						return Double.valueOf(n.key.toString());
					}
				} else {
					return Double.valueOf(n.key.toString());
				}
			}

			if ((n.keyCount + leftKeyCount + n.leftCarry) < (rightKeyCount + n.rightCarry)) {
				if (n.right != null) {
					n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
					n.right.rightCarry = n.rightCarry;
					n.right.parent = n;
				} else {
					if (n.left != null) {
						if (n.key instanceof Integer) {
							return (Integer.valueOf(n.key.toString()) + Integer.valueOf(n.left.key.toString()))
									/ denominator;
						} else if (n.key instanceof Double) {
							return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString()))
									/ denominator;
						}
					}
				}
				return traverseTree(n.right, midPointFound);
			} else if ((n.keyCount + rightKeyCount + n.rightCarry) < (leftKeyCount + n.leftCarry)) {
				if (n.left != null) {
					n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
					n.left.leftCarry = n.leftCarry;
					n.left.parent = n;
				} else {
					if (n.right != null) {
						if (n.key instanceof Integer) {
							return (Integer.valueOf(n.key.toString()) + Integer.valueOf(n.right.key.toString()))
									/ denominator;
						} else if (n.key instanceof Double) {
							return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
									/ denominator;
						}
					}
				}
				return traverseTree(n.left, midPointFound);
			} else if ((n.keyCount + leftKeyCount + n.leftCarry) == (rightKeyCount + n.rightCarry)) {
				if (n.right != null) {
					n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
					n.right.rightCarry = n.rightCarry;
					n.right.parent = n;
				} else {
					if (n.left != null) {
						if (n.key instanceof Integer) {
							return (Integer.valueOf(n.key.toString()) + Integer.valueOf(n.left.key.toString()))
									/ denominator;
						} else if (n.key instanceof Double) {
							return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString()))
									/ denominator;
						}
					}
				}
				if (n.key instanceof Integer) {
					return (Integer.valueOf(n.key.toString()) + traverseTree(n.right, true)) / denominator;
				} else if (n.key instanceof Double) {
					return (Double.valueOf(n.key.toString()) + traverseTree(n.right, true)) / denominator;
				}
			} else if ((n.keyCount + rightKeyCount + n.rightCarry) == (leftKeyCount + n.leftCarry)) {
				if (n.left != null) {
					n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
					n.left.leftCarry = n.leftCarry;
					n.left.parent = n;
				} else {
					if (n.right != null) {
						if (n.key instanceof Integer) {
							return (Integer.valueOf(n.key.toString()) + Integer.valueOf(n.right.key.toString()))
									/ denominator;
						} else if (n.key instanceof Double) {
							return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
									/ denominator;
						}
					}
				}
				if (n.key instanceof Integer) {
					return (Integer.valueOf(n.key.toString()) + traverseTree(n.left, true)) / denominator;
				} else if (n.key instanceof Double) {
					return (Double.valueOf(n.key.toString()) + traverseTree(n.left, true)) / denominator;
				}
			} else {
				if (n.key instanceof Integer) {
					return Integer.valueOf(n.key.toString());
				} else if (n.key instanceof Double) {
					return Double.valueOf(n.key.toString());
				}
			}
		}
		return 0.0;
	}

	/**
	 * @param n
	 *            : Particular node of the AVL tree.
	 * @return Returns the total count of the left and right child with
	 *         duplicates.
	 */
	private int childKeyCount(Node n) {
		if (n == null) {
			return 0;
		} else {
			int leftKeyCount = childKeyCount(n.left);
			int rightKeyCount = childKeyCount(n.right);
			return leftKeyCount + rightKeyCount + n.keyCount;
		}
	}

	private Node insert(Node parent, T key) {
		if (parent == null) {
			return new Node(key);
		}
		boolean balancingNeeded = false;
		if (key.compareTo(parent.key) < 0) {
			parent.left = insert(parent.left, key);
			balancingNeeded = true;
		} else if (key.compareTo(parent.key) > 0) {
			parent.right = insert(parent.right, key);
			balancingNeeded = true;
		} else {
			parent.keyCount++;
			balancingNeeded = false;
		}
		return balancingNeeded ? balance(parent) : parent;
	}

	private Node balance(Node n) {
		fixHeightAndChildCount(n);
		if (bfactor(n) == 2) {
			if (bfactor(n.right) < 0) {
				n.right = rotateRight(n.right);
			}
			return rotateLeft(n);
		}
		if (bfactor(n) == -2) {
			if (bfactor(n.left) > 0) {
				n.left = rotateLeft(n.left);
			}
			return rotateRight(n);
		}
		return n;
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

	private void fixHeightAndChildCount(Node n) {
		int hl = height(n.left);
		int hr = height(n.right);
		n.height = (hl > hr ? hl : hr) + 1;
		n.childCount = 0;
		if (n.left != null) {
			n.childCount = n.left.childCount + 1;
		}
		if (n.right != null) {
			n.childCount += n.right.childCount + 1;
		}
	}

	private int height(Node n) {
		return n == null ? 0 : n.height;
	}

	private int bfactor(Node n) {
		return height(n.right) - height(n.left);
	}

	public void insert(T... keys) {
		for (T key : keys) {
			root = insert(root, key);
		}
	}

	private class Node {

		private Node left;
		private Node right;
		private final T key;
		private int height;
		private int childCount;
		private int keyCount = 0;
		private int leftCarry = 0;
		private int rightCarry = 0;
		private Node parent;

		private Node(T value) {
			key = value;
			height = 1;
			keyCount++;
		}
	}

}

/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryhippos.ds;

/**
 * @author pooshans
 *
 */
public abstract class AbstractDescriptiveStatistics<T extends Comparable<? super T>> {
	protected Node<T> root;
	protected int totalCount;
	protected final static double DENOMINATOR = 2.0;
	private final static int SLIDING_WINDOW_SIZE = -1;

	public AbstractDescriptiveStatistics() {
	}

	public AbstractDescriptiveStatistics(T[] keys) {
		if (keys == null || keys.length == 0) {
			throw new IllegalArgumentException("Null or empty array");
		}
		this.totalCount = keys.length;
		insert(keys);
	}

	/**
	 * @return Returns the median.
	 */
	public abstract Comparable percentile(int percentile);

	/**
	 * @return Total unique keys
	 */
	public int unique() {
		return startUniqueCount(root);
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
	 *            : Particular node of the AVL tree.
	 * @return Returns the total count of the left and right child with
	 *         duplicates.
	 */
	protected int childKeyCount(Node<T> n) {
		if (n == null) {
			return 0;
		} else {
			int leftKeyCount = childKeyCount(n.left);
			int rightKeyCount = childKeyCount(n.right);
			return leftKeyCount + rightKeyCount + n.keyCount;
		}
	}

	private Node<T> insert(Node<T> parent, T key) {
		if (parent == null) {
			return new Node<T>(key);
		}
		boolean balancingNeeded = false;
		if (key.compareTo((T) (parent.key)) < 0) {
			parent.left = insert(parent.left, key);
			balancingNeeded = true;
		} else if (key.compareTo((T) parent.key) > 0) {
			parent.right = insert(parent.right, key);
			balancingNeeded = true;
		} else {
			parent.keyCount++;
			balancingNeeded = false;
		}
		return balancingNeeded ? balance(parent) : parent;
	}

	private Node<T> balance(Node<T> n) {
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

	private Node<T> rotateRight(Node<T> p) {
		Node<T> q = p.left;
		p.left = q.right;
		q.right = p;
		fixHeightAndChildCount(p);
		fixHeightAndChildCount(q);
		return q;
	}

	private Node<T> rotateLeft(Node<T> q) {
		Node<T> p = q.right;
		q.right = p.left;
		p.left = q;
		fixHeightAndChildCount(q);
		fixHeightAndChildCount(p);
		return p;
	}

	private void fixHeightAndChildCount(Node<T> n) {
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

	private Node<T> delete(Node<T> node, T key) {
		if (node == null) {
			throw new IllegalArgumentException("There is no element.");
		}
		int comp = node.key.compareTo(key);
		if (comp == 0) {
			if (node.left == null && node.right == null) {
				return null;
			} else if (node.left == null) {
				return node.right;
			} else if (node.right == null) {
				return node.left;
			} else {
				Node<T> left = node.right;
				while (left.left != null) {
					left = left.left;
				}
				left.right = deleteLeft(node.right);
				left.left = node.left;
				return balance(left);
			}
		} else if (comp < 0) {
			node.left = delete(node.left, key);
		} else {
			node.right = delete(node.right, key);
		}
		return balance(node);
	}

	private Node<T> deleteLeft(Node<T> node) {
		if (node.left == null) {
			return node.right;
		} else {
			node.left = deleteLeft(node.left);
			return balance(node);
		}
	}

	public void delete(T key) {
		root = delete(root, key);
	}

	private int height(Node<T> n) {
		return n == null ? 0 : n.height;
	}

	private int bfactor(Node<T> n) {
		return height(n.right) - height(n.left);
	}

	public void insert(T[] keys) {
		for (T key : keys) {
			root = insert(root, key);
		}
	}

	private int startUniqueCount(Node<T> n) {
		if (n == null) {
			return 0;
		}
		int leftChildCount = startUniqueCount(n.left);
		int rightChildCount = startUniqueCount(n.right);
		return leftChildCount + rightChildCount + 1;
	}

	protected class Node<T> {
		protected Node<T> left;
		protected Node<T> right;
		protected final T key;
		protected int height;
		protected int childCount;
		protected int keyCount = 0;
		protected int leftCarry = 0;
		protected int rightCarry = 0;
		protected Node<T> parent;

		private Node(T value) {
			key = value;
			height = 1;
			keyCount++;
		}
	}
}

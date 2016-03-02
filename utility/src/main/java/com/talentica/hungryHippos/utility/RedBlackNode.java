package com.talentica.hungryHippos.utility;

/**
 * Red black node of the tree
 * @author PooshanS
 *
 * @param <T>
 */
public class RedBlackNode<T extends Comparable<T>, V> {

    public static final int BLACK = 0;
    public static final int RED = 1;
	// the key of each node
	private T key;

	private V value;

	public T getKey() {
		return key;
	}

	public void setKeyValue(T key, V value) {
		this.key = key;
		this.value = value;
	}

	public V getValue() {
		return value;
	}

    /** Parent of node */
	RedBlackNode<T, V> parent;
    /** Left child */
	RedBlackNode<T, V> left;
    /** Right child */
	RedBlackNode<T, V> right;
    // the number of elements to the left of each node
    public int numLeft = 0;
    // the number of elements to the right of each node
    public int numRight = 0;
    // the color of a node
    public int color;

    RedBlackNode(){
        color = BLACK;
        numLeft = 0;
        numRight = 0;
        parent = null;
        left = null;
        right = null;
    }

	RedBlackNode(T key, V value) {
        this();
		this.value = value;
        this.key = key;
	}
}


package com.talentica.hungryhippos.ds;

/**
 * This is the median calculator using AVL tree.
 * 
 * @author pooshans
 *
 */
public class DescriptiveStatisticsNumber<T extends Number> extends AbstractDescriptiveStatistics {

    private int percentile;

    public <T extends Number> DescriptiveStatisticsNumber() {
    }

    public <T extends Number> DescriptiveStatisticsNumber(Comparable<T>[] keys) {
        super(keys);
    }

    /**
     * @return Returns the median in double.
     */
    @Override
    public Comparable percentile(int percentile) {
        if (percentile <= 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile should be > 0 and <= 100");
        }
        this.percentile = percentile;
        return traverseTree(root, false);
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
                        return (Double.valueOf(n.key.toString()) + Double.valueOf(n.parent.key.toString()))
                                / DENOMINATOR;
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

            if ((percentile) * (n.keyCount + leftKeyCount + n.leftCarry) < (100 - percentile)
                    * (rightKeyCount + n.rightCarry)) {
                if (n.right != null) {
                    n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
                    n.right.rightCarry = n.rightCarry;
                    n.right.parent = n;
                } else {
                    if (n.left != null) {
                        return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString())) / DENOMINATOR;
                    }
                }
                return traverseTree(n.right, midPointFound);
            } else if ((percentile) * (n.keyCount + rightKeyCount + n.rightCarry) < (100 - percentile)
                    * (leftKeyCount + n.leftCarry)) {
                if (n.left != null) {
                    n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
                    n.left.leftCarry = n.leftCarry;
                    n.left.parent = n;
                } else {
                    if (n.right != null) {
                        return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
                                / DENOMINATOR;
                    }
                }
                return traverseTree(n.left, midPointFound);
            } else if ((percentile) * (n.keyCount + leftKeyCount + n.leftCarry) == (100 - percentile)
                    * (rightKeyCount + n.rightCarry)) {
                if (n.right != null) {
                    n.right.leftCarry = n.keyCount + leftKeyCount + n.leftCarry;
                    n.right.rightCarry = n.rightCarry;
                    n.right.parent = n;
                } else {
                    if (n.left != null) {
                        return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString())) / DENOMINATOR;
                    }
                }
                return (Double.valueOf(n.key.toString()) + traverseTree(n.right, true)) / DENOMINATOR;
            } else if ((percentile) * (n.keyCount + rightKeyCount + n.rightCarry) == (100 - percentile)
                    * (leftKeyCount + n.leftCarry)) {
                if (n.left != null) {
                    n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
                    n.left.leftCarry = n.leftCarry;
                    n.left.parent = n;
                } else {
                    if (n.right != null) {
                        return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
                                / DENOMINATOR;
                    }
                }
                return (Double.valueOf(n.key.toString()) + traverseTree(n.left, true)) / DENOMINATOR;
            } else {
                return Double.valueOf(n.key.toString());
            }
        }
    }

}

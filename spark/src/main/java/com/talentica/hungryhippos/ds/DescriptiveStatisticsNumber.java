package com.talentica.hungryhippos.ds;

/**
 * This is the median calculator using AVL tree.
 * 
 * @author pooshans
 *
 */
public class DescriptiveStatisticsNumber<T extends Number> extends AbstractDescriptiveStatistics {

  public <T extends Number> DescriptiveStatisticsNumber() {}

  public <T extends Number> DescriptiveStatisticsNumber(Comparable<T>[] keys) {
    super(keys);
    insert(keys);
  }

  /**
   * @return Returns the median.
   */
  public double median() {
    return traverseTree(root, false);
  }


  /**
   * @param n : It is the root of the AVL tree form where the actual traversal start for median
   *        search.
   * @param midPointFound : It the identifier which signal that one key is found in mid point and
   *        start looking for other one.
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
                / denominator;
          } else {
            /*
             * mid point found at current node and it's duplicate value, simply return it.
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
            return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString()))
                / denominator;
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
            return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
                / denominator;
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
            return (Double.valueOf(n.key.toString()) + Double.valueOf(n.left.key.toString()))
                / denominator;
          }
        }
        return (Double.valueOf(n.key.toString()) + traverseTree(n.right, true)) / denominator;
      } else if ((n.keyCount + rightKeyCount + n.rightCarry) == (leftKeyCount + n.leftCarry)) {
        if (n.left != null) {
          n.left.rightCarry = n.keyCount + rightKeyCount + n.rightCarry;
          n.left.leftCarry = n.leftCarry;
          n.left.parent = n;
        } else {
          if (n.right != null) {
            return (Double.valueOf(n.key.toString()) + Double.valueOf(n.right.key.toString()))
                / denominator;
          }
        }
        return (Double.valueOf(n.key.toString()) + traverseTree(n.left, true)) / denominator;
      } else {
        return Double.valueOf(n.key.toString());
      }
    }
  }

}

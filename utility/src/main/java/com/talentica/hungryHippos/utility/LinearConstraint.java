package com.talentica.hungryHippos.utility;

/**
 * Created by debasishc on 13/8/15.
 */
public class LinearConstraint {
    private double [] coefficients;
    private double rhs;
    private ConstraintType constraintType;

    public LinearConstraint(double[] coefficients, ConstraintType constraintType, double rhs) {
        this.coefficients = coefficients;
        this.constraintType = constraintType;
        this.rhs = rhs;
    }

    public double[] getCoefficients() {
        return coefficients;
    }

    public void setCoefficients(double[] coefficients) {
        this.coefficients = coefficients;
    }

    public ConstraintType getConstraintType() {
        return constraintType;
    }

    public void setConstraintType(ConstraintType constraintType) {
        this.constraintType = constraintType;
    }

    public double getRhs() {
        return rhs;
    }

    public void setRhs(double rhs) {
        this.rhs = rhs;
    }
}

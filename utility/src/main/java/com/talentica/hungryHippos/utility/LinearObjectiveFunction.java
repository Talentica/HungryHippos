package com.talentica.hungryHippos.utility;

/**
 * Created by debasishc on 13/8/15.
 */
public class LinearObjectiveFunction {
    private double[] coefficients;

    public LinearObjectiveFunction(double[] coefficients) {
        this.coefficients = coefficients;
    }

    public double[] getCoefficients() {
        return coefficients;
    }

    public void setCoefficients(double[] coefficients) {
        this.coefficients = coefficients;
    }
}

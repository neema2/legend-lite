package org.finos.legend.pure.m3;

/**
 * Represents multiplicity constraints on a property.
 * 
 * @param lowerBound Minimum cardinality (0 for optional, 1+ for required)
 * @param upperBound Maximum cardinality (null represents unbounded/*)
 */
public record Multiplicity(int lowerBound, Integer upperBound) {
    
    /** Single required value [1] */
    public static final Multiplicity ONE = new Multiplicity(1, 1);
    
    /** Optional single value [0..1] */
    public static final Multiplicity ZERO_ONE = new Multiplicity(0, 1);
    
    /** Alias for ZERO_ONE */
    public static final Multiplicity ZERO_OR_ONE = ZERO_ONE;
    
    /** Zero or more [*] */
    public static final Multiplicity MANY = new Multiplicity(0, null);
    
    /** One or more [1..*] */
    public static final Multiplicity ONE_MANY = new Multiplicity(1, null);
    
    public Multiplicity {
        if (lowerBound < 0) {
            throw new IllegalArgumentException("Lower bound cannot be negative");
        }
        if (upperBound != null && upperBound < lowerBound) {
            throw new IllegalArgumentException("Upper bound cannot be less than lower bound");
        }
    }
    
    public boolean isRequired() {
        return lowerBound >= 1;
    }
    
    public boolean isUnbounded() {
        return upperBound == null;
    }
    
    public boolean isSingular() {
        return upperBound != null && upperBound == 1;
    }
    
    @Override
    public String toString() {
        if (upperBound == null) {
            return lowerBound == 0 ? "[*]" : "[" + lowerBound + "..*]";
        }
        if (lowerBound == upperBound) {
            return "[" + lowerBound + "]";
        }
        return "[" + lowerBound + ".." + upperBound + "]";
    }
}

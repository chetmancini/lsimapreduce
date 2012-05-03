package org.lsi.containers;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import static org.junit.Assert.*;
/**
 * Unit test for simple App.
 */
public class ComplexNumberTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ComplexNumberTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( ComplexNumberTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testComplexNumberLessThan()
    {
        int expected = -1; 
        ComplexNumber a = new ComplexNumber(0,0);
        ComplexNumber b = new ComplexNumber(0,1);

        assertEquals(expected,a.compareTo(b));
    }
    public void testComplexNumberEquals()
    {
        int expected = 0;
        ComplexNumber a = new ComplexNumber(1,1);
        ComplexNumber b = new ComplexNumber(1,1);

        assertEquals(expected,a.compareTo(b));
    }
    public void testComplexNumberGreaterThan()
    {
        int expected = 1;
        ComplexNumber a = new ComplexNumber(0,1);
        ComplexNumber b = new ComplexNumber(0,0);

        assertEquals(expected,a.compareTo(b));
    }
    
}

package org.apache.derby.iapi.sql.execute.xplain;
import org.apache.derby.iapi.error.StandardException;
/**
 * This is the factory interface of the XPLAINFactory facility. It extends the 
 * possibilities and provides a convenient protocol to explain queries 
 * on basis of the query execution plan. This plan manfifests in Derby in the 
 * different ResultSets and their associated statistics. The introduction of 
 * this factory interface makes it possible to switch to another implementation 
 * or to easily extend the API.
 *  
 */
public interface XPLAINFactoryIF {

    /**
    Module name for the monitor's module locating system.
    */
    String MODULE = "org.apache.derby.iapi.sql.execute.xplain.XPLAINFactoryIF";
    
    /**
     * This method returns an appropriate visitor to traverse the 
     * ResultSetStatistics. Depending on the current configuration, 
     * the perfect visitor will be chosen, created and cached by this factory
     * method. 
     * @return a XPLAINVisitor to traverse the ResultSetStatistics
     * @see XPLAINVisitor
     */
    public XPLAINVisitor getXPLAINVisitor() throws StandardException;
    
    /**
     * This method gets called when the user switches off the explain facility.
     * The factory destroys for example the cached visitor implementation(s) or 
     * releases resources to save memory.
     */
    public void freeResources();
    
}

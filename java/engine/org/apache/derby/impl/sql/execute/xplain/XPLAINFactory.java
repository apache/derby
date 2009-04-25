package org.apache.derby.impl.sql.execute.xplain;

import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINFactoryIF;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

/**
 * This is the module implementation of the XPLAINFactoryIF. It gets lazy-loaded
 * when needed. The factory method determines which visitor to use. 
 * The visitor is cached in this factory for later reuse. 
 *
 */
public class XPLAINFactory implements XPLAINFactoryIF {
    
    /** the last instance of a visitor is cached */
    private XPLAINVisitor currentVisitor = new XPLAINDefaultVisitor();
    
    /** the current cached schema */ 
    private String       currentSchema = null;
    
    public XPLAINFactory(){
    }
    
    /**
     * the factory method, which gets called to determine 
     * and return an appropriate XPLAINVisitor instance
     */
    public XPLAINVisitor getXPLAINVisitor()
        throws StandardException
    {
        try
        {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            String schema = lcc.getXplainSchema();
            if (schema != currentSchema)
            {
                currentSchema = schema;
                if (currentSchema == null)
                    currentVisitor = new XPLAINDefaultVisitor();
                else
                    currentVisitor = new XPLAINSystemTableVisitor();
            }
        }
        catch (SQLException e)
        {
            throw StandardException.plainWrapException(e);
        }
        return currentVisitor;
    }

    /**
     * uncache the visitor and reset the factory state
     */
    public void freeResources() {
        // let the garbage collector destroy the visitor and schema
        currentVisitor = null;
        currentSchema = null;
    }

}

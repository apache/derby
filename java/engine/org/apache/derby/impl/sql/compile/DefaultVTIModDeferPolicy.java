/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.vti.DeferModification;

/**
 * This class implements the default policy for defering modifications to virtual
 * tables.
 */
class DefaultVTIModDeferPolicy implements DeferModification
{
	/**
		IBM Copyright &copy notice.
	*/

    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;
    private final String targetVTIClassName;
    private final boolean VTIResultSetIsSensitive;

    DefaultVTIModDeferPolicy( String targetVTIClassName,
                              boolean VTIResultSetIsSensitive)
    {
        this.targetVTIClassName = targetVTIClassName;
        this.VTIResultSetIsSensitive = VTIResultSetIsSensitive;
    }

    /**
     * @see org.apache.derby.vti.DeferModification#alwaysDefer
     */
    public boolean alwaysDefer( int statementType)
    {
        return false;
    }
          
    /**
     * @see org.apache.derby.vti.DeferModification#columnRequiresDefer
     */
    public boolean columnRequiresDefer( int statementType,
                                        String columnName,
                                        boolean inWhereClause)
    {
        switch( statementType)
        {
        case DeferModification.INSERT_STATEMENT:
            return false;

        case DeferModification.UPDATE_STATEMENT:
            return VTIResultSetIsSensitive && inWhereClause;

        case DeferModification.DELETE_STATEMENT:
            return false;
        }
        return false; // Should not get here.
    } // end of columnRequiresDefer

    /**
     * @see org.apache.derby.vti.DeferModification#subselectRequiresDefer(int,String,String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String schemaName,
                                           String tableName)
    {
        return false;
    } // end of subselectRequiresDefer( statementType, schemaName, tableName)

    /**
     * @see org.apache.derby.vti.DeferModification#subselectRequiresDefer(int, String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String VTIClassName)
    {
        return targetVTIClassName.equals( VTIClassName);
    } // end of subselectRequiresDefer( statementType, VTIClassName)

    public void modificationNotify( int statementType,
                                    boolean deferred)
    {}
}

/*

   Derby - Class org.apache.derby.impl.sql.compile.VTIDeferModPolicy

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package	org.apache.derby.impl.sql.compile;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.vti.DeferModification;

/**
 * This class applies a VTI modification deferral policy to a statement to
 * see whether it should be deferred.
 */
class VTIDeferModPolicy implements Visitor
{
    /**
     * See if a VTI modification statement should be deferred.
     *
     * @param statementType DeferModification.INSERT_STATEMENT, UPDATE_STATEMENT, or DELETE_STATEMENT
     * @param targetVTI The target VTI
     * @param updateColumnNames The list of columns being updated, null if this is not an update statement
     * @param source
     */
    public static boolean deferIt( int statementType,
                                   FromVTI targetVTI,
                                   String[] updateColumnNames,
                                   QueryTreeNode source)
        throws StandardException
    {
        try
        {
            DeferModification deferralControl;
            int resultSetType = targetVTI.getResultSetType( );

            /* Deferred updates and deletes are implemented by scrolling the result set. So, if
             * the statement is an update or delete but the result set is not scrollable then do
             * not attempt to defer the statement.
             */
            if( (statementType == DeferModification.UPDATE_STATEMENT ||statementType == DeferModification.DELETE_STATEMENT)
                && resultSetType == ResultSet.TYPE_FORWARD_ONLY)
                return false;

            deferralControl = targetVTI.getDeferralControl();
            if( deferralControl == null)
            {
                String VTIClassName = targetVTI.getMethodCall().getJavaClassName();
                deferralControl = new DefaultVTIModDeferPolicy( VTIClassName,
                                                                ResultSet.TYPE_SCROLL_SENSITIVE == resultSetType);
            }
            if( deferralControl.alwaysDefer( statementType))
                return true;

            if( source == null && statementType != DeferModification.UPDATE_STATEMENT)
                return false;

            VTIDeferModPolicy deferralSearch = new VTIDeferModPolicy( targetVTI,
                                                                      updateColumnNames,
                                                                      deferralControl,
                                                                      statementType);

            if( source != null)
                source.accept( deferralSearch);

            if( statementType == DeferModification.UPDATE_STATEMENT)
            {
                // Apply the columnRequiresDefer method to updated columns not in the where clause.
                for (String s : deferralSearch.columns) {
                    if (deferralControl.columnRequiresDefer(
                            statementType, s, false)) {
                        return true;
                    }
                }
            }
            return deferralSearch.deferred;
        }
        catch( SQLException sqle)
        {
            throw StandardException.unexpectedUserException(sqle);
        }
    } // end of deferIt

    // state needed to search the statement parse tree for nodes that require deferred modification
    private boolean deferred = false;
    private DeferModification deferralControl;
    private int statementType;
    private int tableNumber;
    private final HashSet<String> columns = new HashSet<String>();

    private VTIDeferModPolicy( FromVTI targetVTI,
                               String[] columnNames,
                               DeferModification deferralControl,
                               int statementType)
    {
        this.deferralControl = deferralControl;
        this.statementType = statementType;
        tableNumber = targetVTI.getTableNumber();
        if( statementType == DeferModification.UPDATE_STATEMENT && columnNames != null)
        {
            columns.addAll(Arrays.asList(columnNames));
        }
    }

    public Visitable visit(Visitable node)
        throws StandardException
    {
        try
        {
            if( node instanceof ColumnReference && statementType != DeferModification.INSERT_STATEMENT)
            {
                ColumnReference cr = (ColumnReference) node;
                if( cr.getTableNumber() == tableNumber)
                {
                    String columnName = cr.getColumnName();
                    if( statementType == DeferModification.DELETE_STATEMENT)
                    {
                        if (columns.add(columnName))
                        {
                            if( deferralControl.columnRequiresDefer( statementType, columnName, true))
                                deferred = true;
                        }
                    }
                    else if( statementType == DeferModification.UPDATE_STATEMENT)
                    {
                        if (columns.remove(columnName))
                        {
                            // This column is referenced in the where clause and is being updated
                            if( deferralControl.columnRequiresDefer( statementType, columnName, true))
                                deferred = true;
                        }
                    }
                }
            }
            else if( node instanceof SelectNode)
            {
                SelectNode subSelect = (SelectNode) node;
                FromList fromList = subSelect.getFromList();

                for( int i = 0; i < fromList.size(); i++)
                {
                    FromTable fromTable = (FromTable) fromList.elementAt(i);
                    if( fromTable instanceof FromBaseTable)
                    {
                        TableDescriptor td = fromTable.getTableDescriptor();
                        if( deferralControl.subselectRequiresDefer( statementType,
                                                                    td.getSchemaName(),
                                                                    td.getName()))
                            deferred = true;
                    }
                    else if( fromTable instanceof FromVTI)
                    {
                        FromVTI fromVTI = (FromVTI) fromTable;
                        if( deferralControl.subselectRequiresDefer( statementType,
                                                                    fromVTI.getMethodCall().getJavaClassName()))
                            deferred = true;
                    }
                }
            }
        }
        catch( SQLException sqle)
        {
            throw StandardException.unexpectedUserException(sqle);
        }
        return node;
    } // end of visit
    
    public boolean stopTraversal()
    {
        return deferred;
    } // end of stopTraversal
    
    public boolean skipChildren(Visitable node)
    {
        return false;
    } // end of skipChildren

    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }
} // end of class VTIDeferModPolicy

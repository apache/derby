/*

   Derby - Class org.apache.derby.iapi.sql.compile.OptimizerPlan

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.UniqueTupleDescriptor;
import org.apache.derby.iapi.util.IdUtil;

/**
 * <p>
 * High level description of a plan for consideration by the Optimizer.
 * This is used to specify a complete plan via optimizer overrides. A
 * plan is a tree whose interior nodes are join operators and whose
 * leaves are row sources (conglomerates or tableFunctions).
 * </p>
 */
public abstract class OptimizerPlan
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	FACTORY METHODS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Make a RowSource corresponding to the given tuple descriptor.
     * </p>
     */
    public  static  RowSource   makeRowSource( UniqueTupleDescriptor utd, DataDictionary dd )
        throws StandardException
    {
        if ( utd == null ) { return null; }
        else if ( utd instanceof ConglomerateDescriptor )
        {
            return new ConglomerateRS( (ConglomerateDescriptor) utd, dd );
        }
        else if ( utd instanceof AliasDescriptor )
        {
            return new TableFunctionRS( (AliasDescriptor) utd );
        }
        else { return null; }
    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	ABSTRACT BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Bind the conglomerate and table function names in this plan.
     * </p>
     *
     * @param   dataDictionary  DataDictionary to bind against.
     */
    public abstract    void    bind
        (
         DataDictionary dataDictionary,
         LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
         CompilerContext cc
         )
        throws StandardException;

    /**
     * <p>
     * Return true if this the schema and RowSource names have been resolved.
     * </p>
     */
    public abstract boolean isBound();

    /**
     * <p>
     * Count the number of leaf nodes under (and including) this node.
     * </p>
     */
    public abstract    int countLeafNodes();
    
    /**
     * <p>
     * Get the leftmost leaf node in this plan.
     * </p>
     */
    public abstract    OptimizerPlan    leftmostLeaf();
    
    /**
     * <p>
     * Return true if this plan is a (left) leading prefix of the other plan.
     * </p>
     */
    public abstract    boolean  isLeftPrefixOf( OptimizerPlan that );
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	INNER CLASSES
    //
    ////////////////////////////////////////////////////////////////////////

    public static  final   class Join extends OptimizerPlan
    {
        final JoinStrategy    strategy;
        final OptimizerPlan   leftChild;
        final OptimizerPlan   rightChild;
        private boolean _isBound;
        private int         _leafNodeCount = 0;

        public Join
            (
             JoinStrategy   strategy,
             OptimizerPlan  leftChild,
             OptimizerPlan  rightChild
             )
        {
            this.strategy = strategy;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }

        public void    bind
            (
             DataDictionary dataDictionary,
             LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
             CompilerContext cc
             )
            throws StandardException
        {
            // only left-deep trees allowed at this time
            if ( !( rightChild instanceof RowSource ) )
            {
                throw StandardException.newException( SQLState.LANG_NOT_LEFT_DEEP );
            }

            leftChild.bind( dataDictionary, lcc, cc );
            rightChild.bind( dataDictionary, lcc, cc );

            _isBound = true;
        }
        
        public boolean isBound() { return _isBound; }
        
        public int countLeafNodes()
        {
            if ( _leafNodeCount <= 0 ) { _leafNodeCount = leftChild.countLeafNodes() + rightChild.countLeafNodes(); }
            return _leafNodeCount;
        }

        public OptimizerPlan    leftmostLeaf()   { return leftChild.leftmostLeaf(); }
        
        public boolean  isLeftPrefixOf( OptimizerPlan other )
        {
            if ( !(other instanceof Join) ) { return false; }

            Join    that = (Join) other;
            
            int thisLeafCount = this.countLeafNodes();
            int thatLeafCount = that.countLeafNodes();

            if ( thisLeafCount > thatLeafCount ) { return false; }
            else if ( thisLeafCount < thatLeafCount ) { return isLeftPrefixOf( that.leftChild ); }
            else { return this.equals( that ); }
        }
        
        public  String  toString()
        {
            return
                "( " +
                leftChild.toString() +
                " " + strategy.getOperatorSymbol() + " " +
                rightChild.toString() +
                " )";
        }

        public  boolean equals( Object other )
        {
            if ( other == null ) { return false; }
            if ( !(other instanceof Join) ) { return false; }

            Join    that = (Join) other;

            if ( !this.strategy.getOperatorSymbol().equals( that.strategy.getOperatorSymbol() ) ) { return false; }

            return this.leftChild.equals( that.leftChild) && this.rightChild.equals( that.rightChild );
        }
    }

    /** Generic plan for row sources we don't understand */
    public static  class    DeadEnd extends OptimizerPlan
    {
        private String  _name;

        public DeadEnd( String name )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
            _name = name;
        }

        public void    bind
            (
             DataDictionary dataDictionary,
             LanguageConnectionContext lcc,
             CompilerContext cc
             )
            throws StandardException
        {}
        
        public boolean isBound() { return true; }

        public int countLeafNodes()    { return 1; }

        public OptimizerPlan    leftmostLeaf()   { return this; }
        
        public boolean  isLeftPrefixOf( OptimizerPlan that )
        {
            return this.equals( that.leftmostLeaf() );
        }
        
        public  String  toString()  { return _name; }
    }

    public abstract    static  class   RowSource<D extends UniqueTupleDescriptor>   extends OptimizerPlan
    {
        protected   String  _schemaName;
        protected   String  _rowSourceName;
        protected   SchemaDescriptor    _schema;
        protected D   _descriptor;

        public RowSource( String schemaName, String rowSourceName )
        {
            _schemaName = schemaName;
            _rowSourceName = rowSourceName;
        }
        protected   RowSource() {}

        /** Get the UniqueTupleDescriptor bound to this RowSource */
        public D   getDescriptor() { return _descriptor; }
        
        public void    bind
            (
             DataDictionary dataDictionary,
             LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
             CompilerContext cc
             )
            throws StandardException
        {
            // bind the schema name
            if ( _schema == null )
            {
                _schema = StatementUtil.getSchemaDescriptor( _schemaName, true, dataDictionary, lcc, cc );
                _schemaName = _schema.getSchemaName();
            }
        }
        
        public boolean isBound() { return (_descriptor != null); }

        public int countLeafNodes()    { return 1; }

        public OptimizerPlan    leftmostLeaf()   { return this; }
        
        public boolean  isLeftPrefixOf( OptimizerPlan that )
        {
            return this.equals( that.leftmostLeaf() );
        }
        
        public  String  toString()
        {
            return IdUtil.mkQualifiedName( _schemaName, _rowSourceName );
        }

        public  boolean equals( Object other )
        {
            if ( other == null ) { return false; }
            if ( other.getClass() != this.getClass() ) { return false; }

            RowSource   that = (RowSource) other;

            if ( !( this.isBound() && that.isBound() ) ) { return false; }

            return this._schemaName.equals( that._schemaName ) && this._rowSourceName.equals( that._rowSourceName );
        }
    }

    public static  final   class   ConglomerateRS  extends RowSource<ConglomerateDescriptor>
    {
        public ConglomerateRS( String schemaName, String rowSourceName ) { super( schemaName, rowSourceName ); }

        public ConglomerateRS( ConglomerateDescriptor cd, DataDictionary dataDictionary )
            throws StandardException
        {
            _descriptor = cd;
            _schema = dataDictionary.getSchemaDescriptor( cd.getSchemaID(), null );
            _schemaName = _schema.getSchemaName();
            _rowSourceName = cd.getConglomerateName();
        }
        
        public void    bind
            (
             DataDictionary dataDictionary,
             LanguageConnectionContext lcc,
             CompilerContext cc
             )
            throws StandardException
        {
            super.bind( dataDictionary, lcc, cc );

            if ( _descriptor == null )
            {
                _descriptor = dataDictionary.getConglomerateDescriptor( _rowSourceName, _schema, false );
            }
            if ( _descriptor == null )
            {
                throw StandardException.newException
                    ( SQLState.LANG_INDEX_NOT_FOUND, _schemaName + "." + _rowSourceName );
            }
        }
    }

    public static  final   class   TableFunctionRS  extends RowSource<AliasDescriptor>
    {
        public TableFunctionRS( String schemaName, String rowSourceName ) { super( schemaName, rowSourceName ); }

        public TableFunctionRS( AliasDescriptor ad )
        {
            _descriptor = ad;
            _schemaName = ad.getSchemaName();
            _rowSourceName = ad.getName();
        }
        
        public void    bind
            (
             DataDictionary dataDictionary,
             LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
             CompilerContext cc
             )
            throws StandardException
        {
            super.bind( dataDictionary, lcc, cc );

            if ( _descriptor == null )
            {
                _descriptor = dataDictionary.getAliasDescriptor
                    ( _schema.getUUID().toString(), _rowSourceName, AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR );
            }
            if ( _descriptor == null )
            {
                throw StandardException.newException
                    (
                     SQLState.LANG_OBJECT_NOT_FOUND,
                     AliasDescriptor.getAliasType( AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR ),
                     _schemaName + "." + _rowSourceName
                     );
            }
        }

        public  String  toString()  { return super.toString() + "()"; }

    }
    
}

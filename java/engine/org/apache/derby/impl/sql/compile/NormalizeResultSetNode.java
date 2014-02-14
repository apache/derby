/*

   Derby - Class org.apache.derby.impl.sql.compile.NormalizeResultSetNode

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

import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.util.JBitSet;

/**
 *
 * A NormalizeResultSetNode represents a normalization result set for any 
 * child result set that needs one.  See non-javadoc comments for 
 * a walk-through of a couple sample code paths.
 */

 /*
 * Below are a couple of sample code paths for NormlizeResultSetNodes.
 * These samples were derived from Army Brown's write-ups attached to DERBY-3310
 * and DERBY-3494.  The text was changed to include the new code path now that 
 * all of the NormalizeResultSetNode  code has been moved into the init() method.
 * There are two sections of code in NormalizeResultSetNode.init() that are relevant:
 * First the code to generate the new node based on the child result set. 
 * We will call this "normalize node creation".
 * 
 *       ResultSetNode rsn  = (ResultSetNode) childResult;
 *       ResultColumnList rcl = rsn.getResultColumns();
 *       ResultColumnList targetRCL = (ResultColumnList) targetResultColumnList;
 *       ...
 *       ResultColumnList prRCList = rcl;
 *       rsn.setResultColumns(rcl.copyListAndObjects());
 *       ...
 *       this.resultColumns = prRCList;
 *
 * Next the code to adjust the types for the NormalizeResultSetNode.  
 * We will call this "type adjustment"
 * 
 *       if (targetResultColumnList != null) {
 *           int size = Math.min(targetRCL.size(), resultColumns.size());
 *           for (int index = 0; index < size; index++) {
 *           ResultColumn sourceRC = (ResultColumn) resultColumns.elementAt(index);
 *           ResultColumn resultColumn = (ResultColumn) targetRCL.elementAt(index);
 *           sourceRC.setType(resultColumn.getTypeServices());
 *           }    
 *           
 * --- Sample 1 : Type conversion from Decimal to BigInt on insert ---  
 * (DERBY-3310 write-up variation) 
 * The SQL statement on which this sample focuses is:
 * 
 * create table d3310 (x bigint);
 * insert into d3310 select distinct * from (values 2.0, 2.1, 2.2) v; 
 * 
 * There are three compilation points of interest for this discussion:
 * 1. Before the "normalize node creation"
 * 2. Before the "type adjustment"
 * 3. After the  "type adjustment"
 * 
 * Upon completion of the "type adjustment", the compilation query 
 * tree is then manipulated during optimization and code generation, the 
 * latter of which ultimately determines how the execution-time ResultSet 
 * tree is going to look.\u00a0 So for this discussion we walk through the query
 * tree as it exists at the various points of interest just described.
 * 
 * 1) To start, the (simplified) query tree that we have looks something like the following:
 * 
 *      InsertNode
 * (RCL_0:ResultColumn_0<BigInt>)
 *           |
 *        SelectNode
 * (RCL_1:ResultColumn_1<Decimal>)
 *           |
 *       FromSubquery
 * (RCL_2:ResultColumn_2<Decimal>)
 *           |
 *        UnionNode
 * (RCL_3:ResultColumn_3<Decimal>)
 * 
 * Notation: In the above tree, node names with "_x" trailing them are used to
 *  distinguish Java Objects from each other. So if ResultColumn_0 shows up 
 *  more than once, then it is the *same* Java object showing up in different 
 *  parts of the query tree.  Type names in angle brackets, such as "<BigInt>",
 *  describe the type of the entity immediately preceding the brackets.  
 *  So a line of the form:
 * 
 *   RCL_0:ResultColumn_0<BigInt>
 * 
 * describes a ResultColumnList object containing one ResultColumn object 
 * whose type is BIGINT. We can see from the above tree that, before 
 * normalize node creation, the top of the compile tree contains an 
 * InsertNode, a SelectNode, a FromSubquery, and a UnionNode, all of 
 * which have different ResultColumnList objects and different ResultColumn 
 * objects within those lists.
 * 
 * 2) After the normalize node creation
 * The childresult passed to the init method of NormalizeResultSetNode is 
 * the InsertNode's child, so it ends up creating a new NormalizeResultSetNode 
 * and putting that node on top of the InsertNode's child--that is, on top of 
 * the SelectNode.
 *
 * At this point it's worth noting that a NormalizeResultSetNode operates 
 * based on two ResultColumnLists: a) its own (call it NRSN_RCL), and b) 
 * the ResultColumnList of its child (call it NRSN_CHILD_RCL). More 
 * specifically, during execution a NormalizeResultSet will take a row 
 * whose column types match the types of NRSN_CHILD_RCL, and it will 
 * "normalize" the values from that row so that they agree with the 
 * types of NRSN_RCL. Thus is it possible--and in fact, it should generally 
 * be the case--that the types of the columns in the NormalizeResultSetNode's 
 * own ResultColumnList are *different* from the types of the columns in 
 * its child's ResultColumnList. That should not be the case for most 
 * (any?) other Derby result set.
 * 
 * So we now have:
 *
 *      InsertNode
 * (RCL_0:ResultColumn_0<BigInt>)
 *          |
 *  NormalizeResultSetNode
 * (RCL_1:ResultColumn_1<Decimal> -> VirtualColumnNode<no_type> -> ResultColumn_4<Decimal>)
 *           |
 *        SelectNode
 * (RCL_4:ResultColumn_4<Decimal>)
 *          |
 *      FromSubquery
 * (RCL_2:ResultColumn_2<Decimal>)
 *          |
 *        UnionNode
 * (RCL_3:ResultColumn_3<Decimal>)
 *
 * Notice how, when we generate the NormalizeResultSetNode, three things happen:
 * 
 * a) The ResultColumList object for the SelectNode is "pulled up" into the 
 * NormalizeResultSetNode.
 * b) SelectNode is given a new ResultColumnList--namely, a clone of its old
 *  ResultColumnList, including clones of the ResultColumn objects.
 * c) VirtualColumnNodes are generated beneath NormalizeResultSetNode's 
 * ResultColumns, and those VCNs point to the *SAME RESULT COLUMN OBJECTS* 
 * that now sit in the SelectNode's new ResultColumnList.  
 * Also note how the generated VirtualColumnNode has no type of its own; 
 * since it is an instance of ValueNode it does have a dataTypeServices 
 * field, but that field was not set when the NormalizeResultSetNode was 
 * created.  Hence "<no_type>" in the above tree.
 * 
 * And finally, note that at this point, NormalizeResultSetNode's 
 * ResultColumnList has the same types as its child's ResultColumnList
 * --so the NormalizeResultSetNode doesn't actually do anything 
 * in its current form.
 * 
 * 3) Within the "type adjustment"
 * 
 * The purpose of the "type adjustment" is to take the types from 
 * the InsertNode's ResultColumnList and "push" them down to the 
 * NormalizeResultSetNode. It is this method which sets NRSN_RCL's types 
 * to match the target (INSERT) table's types--and in doing so, makes them 
 * different from NRSN_CHILD_RCL's types.  Thus this is important because 
 * without it, NormalizeResultSetNode would never change the types of the 
 * values it receives.
 * 
 * That said, after the call to sourceRC.setType(...) we have:
 *
 *      InsertNode
 * (RCL0:ResultColumn_0<BigInt>)
 *           |
 *   NormalizeResultSetNode
 * (RCL1:ResultColumn_1<BigInt> -> VirtualColumnNode_0<no_type> -> ResultColumn_4<Decimal>)
 *           |
 *        SelectNode
 * (RCL4:ResultColumn_4<Decimal>)
 *           |
 *       FromSubquery
 * (RCL2:ResultColumn_2<Decimal>)
 *           |
 *        UnionNode
 * (RCL3:ResultColumn_3<Decimal>)
 *
 * The key change here is that ResultColumn_1 now has a type of BigInt 
 * intead of Decimal.  Since the SelectNode's ResultColumn, ResultColumn_4,
 *  still has a type of Decimal, the NormalizeResulSetNode will take as input
 *  a Decimal value (from SelectNode) and will output that value as a BigInt, 
 *  where output means pass the value further up the tree during execution 
 *  (see below).
 * 
 * Note before the fix for DERBY-3310, there was an additional type change 
 * that caused problems with this case.    
 * See the writeup attached to DERBY-3310 for details on why this was a problem.  
 *  
 * 4) After preprocessing and optimization:
 * 
 * After step 3 above, Derby will move on to the optimization phase, which 
 * begins with preprocessing.  During preprocessing the nodes in the tree 
 * may change shape/content to reflect the needs of the optimizer and/or to 
 * perform static optimizations/rewrites. In the case of our INSERT statement 
 * the preprocessing does not change much:
 *
 *     InsertNode
 * (RCL0:ResultColumn_0<BigInt>)
 *          |
 *  NormalizeResultSetNode
 * (RCL1:ResultColumn_1<BigInt> -> VirtualColumnNode<no_type> -> ResultColumn_4<Decimal>)
 *          |
 *       SelectNode
 * (RCL4:ResultColumn_4<Decimal>)
 *          |
 *   ProjectRestrictNode_0
 * (RCL2:ResultColumn_2<Decimal>)
 *          |
 *       UnionNode
 * (RCL3:ResultColumn_3<Decimal>)
 *
 * The only thing that has changed between this tree and the one shown in 
 * step 3 is that the FromSubquery has been replaced with a ProjectRestrictNode.
 * Note that the ProjectRestrictNode has the same ResultColumnList object as 
 * the FromSubquery, and the same ResultColumn object as well.  That's worth 
 * noting because it's another example of how Java objects can be "moved" 
 * from one node to another during Derby compilation.
 * 
 * 5) After modification of access paths:
 * As the final stage of optimization Derby will go through the modification 
 * of access paths phase, in which the query tree is modified to prepare for 
 * code generation.  When we are done modifying access paths, our tree looks 
 * something like this:

      InsertNode
 (RCL0:ResultColumn_0<BigInt>)
           |
   NormalizeResultSetNode
 (RCL1:ResultColumn_1<BigInt> -> VirtualColumnNode<no_type> -> ResultColumn_4<Decimal>)
           |
      DistinctNode
 (RCL4:ResultColumn_4<Decimal> -> VirtualColumnNode<no_type> -> ResultColumn_5<Decimal>)
           |
     ProjectRestrictNode_1
 (RCL5:ResultColumn_5<Decimal>)
           |
    ProjectRestrictNode_0
 (RCL2:ResultColumn_2<Decimal>)
           |
        UnionNode
 (RCL3:ResultColumn_3<Decimal>)

 * The key thing to note here is that the SelectNode has been replaced with two 
 * new nodes: a ProjectRestrictNode whose ResultColumnList is a clone of the 
 * SelectNode's ResultColumnList, and a DistinctNode, whose ResultColumnList 
 * is the same object as the SelectNode's old ResultColumnList.  More 
 * specifically, all of the following occurred as part of modification of 
 * access paths:
 *  
 * a)    The SelectNode was replaced with ProjectRestrictNode_1, whose 
 * ResultColumnList was the same object as the SelectNode's ResultColumnList.
 *
 * b)    the ResultColumList object for ProjectRestrictNode_1 was pulled up 
 * into a new DistinctNode.
 *
 * c)    ProjectRestrictNode_1 was given a new ResultColumnList--namely, a 
 * clone of its old ResultColumnList, including clones of the ResultColumn 
 * objects.
 * 
 * d)    VirtualColumnNodes were generated beneath the DistinctNode's 
 * ResultColumns, and those VCNs point to the same result column objects 
 * that now sit in ProjectRestrictNode_1's new ResultColumnList.
 * 
 * 6) After code generation:
 *
 * During code generation we will walk the compile-time query tree one final 
 * time and, in doing so, we will generate code to build the execution-time 
 * ResultSet tree. As part of that process the two ProjectRestrictNodes will 
 * be skipped because they are both considered no-ops--i.e. they perform 
 * neither projections nor restrictions, and hence are not needed.  
 * (Note that, when checking to see if a ProjectRestrictNode is a no-op, 
 * column types do *NOT* come into play.)
 *
 * Thus the execution tree that we generate ends up looking something like:
 *
 *     InsertNode
 * (RCL0:ResultColumn_0<BigInt>)
 *           |
 *   NormalizeResultSetNode
 * (RCL1:ResultColumn_1<BigInt> -> VirtualColumnNode<no_type> -> ResultColumn_4<Decimal>)
 *           |
 *      DistinctNode
 * (RCL4:ResultColumn_4<Decimal> -> VirtualColumnNode<no_type> -> ResultColumn_5<Decimal>)
 *           |
 *     ProjectRestrictNode_1
 * (RCL5:ResultColumn_5<Decimal>)
 *          |
 *   ProjectRestrictNode_0
 * (RCL2:ResultColumn_2<Decimal>)
 *           |
 *       UnionNode
 * (RCL3:ResultColumn_3<Decimal>)
 *
 * At code generation the ProjectRestrictNodes will again be removed and the 
 * execution tree will end up looking like this:
 * 
 *    InsertResultSet
 *      (BigInt)
 *          |
 *  NormalizeResultSet
 *      (BigInt)
 *          |
 *   SortResultSet
 *      (Decimal)
 *          |
 *   UnionResultSet
 *      (Decimal)
 *
 * where SortResultSet is generated to enforce the DistinctNode, 
 * and thus expects the DistinctNode's column type--i.e. Decimal.
 * 
 * When it comes time to execute the INSERT statement, then, the UnionResultSet 
 * will create a row having a column whose type is DECIMAL, i.e. an SQLDecimal 
 * value.  The UnionResultSet will then pass that up to the SortResultSet, 
 * who is *also* expecting an SQLDecimal value.  So the SortResultSet is 
 * satisfied and can sort all of the rows from the UnionResultSet.  
 * Then those rows are passed up the tree to the NormalizeResultSet, 
 * which takes the DECIMAL value from its child (SortResultSet) and normalizes 
 * it to a value having its own type--i.e. to a BIGINT.  The BIGINT is then 
 * passed up to InsertResultSet, which inserts it into the BIGINT column 
 * of the target table.  And so the INSERT statement succeeds.
 * 
 * ---- Sample 2 -  NormalizeResultSetNode and  Union (DERBY-3494 write-up variation)
 * Query for discussion
 * 
 *
 * create table t1 (bi bigint, i int);
 *  insert into t1 values (100, 10), (288, 28), (4820, 2);
 *
 * select * from
 *   (select bi, i from t1 union select i, bi from t1) u(a,b) where a > 28;
 *
 *
 * Some things to notice about this query:
 * a) The UNION is part of a subquery.
 * b) This is *not* a UNION ALL; i.e. we need to eliminate duplicate rows.
 * c) The left side of the UNION and the right side of the UNION have 
 * different (but compatible) types: the left has (BIGINT, INT), while the 
 * right has (INT, BIGINT).
 * d) There is a predicate in the WHERE clause which references a column 
 * from the UNION subquery.
 * e) The table T1 has at least one row.
 * All of these factors plays a role in the handling of the query and are 
 * relevant to this discussion.
 * 
 * Building the NormalizeResultSetNode. 
 * When compiling a query, the final stage of optimization in Derby is the 
 * "modification of access paths" phase, in which each node in the query 
 * tree is given a chance to modify or otherwise perform maintenance in 
 * preparation for code generation.  In the case of a UnionNode, a call 
 * to modifyAccessPaths() will bring us to the addNewNodes() method, 
 * which is where the call is made to generate the NormalizeResultSetNode.
 * 
 *
 * if (! columnTypesAndLengthsMatch())
 *           {
 *               treeTop = 
 *               (NormalizeResultSetNode) getNodeFactory().getNode(
 *               C_NodeTypes.NORMALIZE_RESULT_SET_NODE,
 *               treeTop, null, null, Boolean.FALSE,
 *               getContextManager());   
 *            }
 *
 * The fact that the left and right children of the UnionNode have different 
 * types (observation c above) means that the if condition will return 
 * true and thus we will generate a NormalizeResultSetNode above the 
 * UnionNode. At this point (before the NormalizeResultSetNode has been 
 * generated) our (simplified) query tree looks something like the following.
 *  PRN stands for ProjectRestrictNode, RCL stands for ResultColumnList:
 *
 *                      PRN0
 *                     (RCL0)
 *             (restriction: a > 28 {RCL1})
 *                       |
 *                    UnionNode          // <-- Modifying access paths...
 *                     (RCL1)
 *                    /      \
 *                  PRN2     PRN3
 *                    |        |
 *                  PRN4     PRN5
 *                    |        |
 *                    T1       T1
 *
 *
 * where 'a > 28 {RCL1}' means that the column reference A in the predicate a > 28 points to a ResultColumn object in the ResultColumnList that corresponds to "RCL1".  I.e. at this point, the predicate's column reference is pointing to an object in the UnionNode's RCL.
 * "normalize node creation"  will execute:
 *
 *        ResultColumnList prRCList = rcl;
 *       rsn.setResultColumns(rcl.copyListAndObjects());
 *       // Remove any columns that were generated.
 *       prRCList.removeGeneratedGroupingColumns();
 *       ...
 *       prRCList.genVirtualColumnNodes(rsn, rsn.getResultColumns());
 *       
 *       this.resultColumns = prRCList;
 *       
 * to create a NormalizeResultSetNode whose result column list is prRCList.  
 * This gives us:
 *
 *                      PRN0
 *                     (RCL0)
 *             (restriction: a > 28 {RCL1})
 *                       |
 *             NormalizeResultSetNode
 *                     (RCL1)              // RCL1 "pulled up" to NRSN
 *                       |
 *                   UnionNode
 *                     (RCL2)              // RCL2 is a (modified) *copy* of RCL1
 *                    /      \
 *                  PRN2     PRN3
 *                    |        |
 *                  PRN4     PRN5
 *                    |        |
 *                    T1       T1
 *
 * Note how RCL1, the ResultColumnList object for the UnionNode, has now been 
 * *MOVED* so that it belongs to the NormalizeResultSetNode.  So the predicate 
 * a > 28, which (still) points to RCL1, is now pointing to the 
 * NormalizeResultSetNode instead of to the UnionNode.
 * 
 * After this, we go back to UnionNode.addNewNodes() where we see the following:
 * 
 *
 *  treeTop = (ResultSetNode) getNodeFactory().getNode(
 *                   C_NodeTypes.DISTINCT_NODE,
 *                   treeTop.genProjectRestrict(),
 *                   Boolean.FALSE,
 *                   tableProperties,
 *                   getContextManager());
 *
 *
 * I.e. we have to generate a DistinctNode to eliminate duplicates because the query 
 * specified UNION, not UNION ALL.
 * 
 * Note the call to treeTop.genProjectRestrict().  Since NormalizeResultSetNode 
 * now sits on top of the UnionNode, treeTop is a reference to the 
 * NormalizeResultSetNode.  That means we end up at the genProjectRestrict() 
 * method of NormalizeResultSetNode.  And guess what?  The method does 
 * something very similar to what we did in NormalizeResultSetNode.init(), 
 * namely:
 *
 *   ResultColumnList prRCList = resultColumns;
 *   resultColumns = resultColumns.copyListAndObjects();
 *
 * and then creates a ProjectRestrictNode whose result column list is prRCList.  This gives us:
 *
 *                     PRN0
 *                    (RCL0)
 *             (restriction: a > 28 {RCL1})
 *                       |
 *                     PRN6
 *                    (RCL1)              // RCL1 "pulled up" to new PRN.
 *                       |
 *             NormalizeResultSetNode
 *                    (RCL3)              // RCL3 is a (modified) copy of RCL1
 *                       |
 *                   UnionNode
 *                     (RCL2)             // RCL2 is a (modified) copy of RCL1
 *                    /      \
 *                  PRN2     PRN3
 *                    |        |
 *                  PRN4     PRN5
 *                    |        |
 *                    T1       T1
 *
 * On top of that we then put a DistinctNode.  And since the init() method 
 * of DistinctNode does the same kind of thing as the previously-discussed 
 * methods, we ultimatley end up with:
 *
 *                     PRN0
 *                     (RCL0)
 *             (restriction: a > 28 {RCL1})
 *                       |
 *                  DistinctNode
 *                    (RCL1)              // RCL1 pulled up to DistinctNode
 *                       |
 *                     PRN6
 *                    (RCL4)              // RCL4 is a (modified) copy of RCL1
 *                       |
 *             NormalizeResultSetNode
 *                    (RCL3)              // RCL3 is a (modified) copy of RCL1
 *                       |
 *                   UnionNode
 *                     (RCL2)             // RCL2 is a (modified) copy of RCL1
 *                    /      \
 *                  PRN2     PRN3
 *                    |        |
 *                  PRN4     PRN5
 *                    |        |
 *                    T1       T1
 *
 * And thus the predicate a > 28, which (still) points to RCL1, is now 
 * pointing to the DistinctNode instead of to the UnionNode. And this 
 * is what we want: i.e. we want the predicate a > 28 to be applied 
 * to the rows that we retrieve from the node at the *top* of the 
 * subtree generated for the UnionNode. It is the non-intuitive code 
 * in the normalize node creation that allows this to happen.
 *
 *
 */

class NormalizeResultSetNode extends SingleChildResultSetNode
{
	/**
	 * this indicates if the normalize is being performed for an Update
	 * statement or not. The row passed to update also has
	 * before values of the columns being updated-- we need not 
	 * normalize these values. 
	 */
	private boolean forUpdate;

	/**
     * Constructor  for a NormalizeResultSetNode.
     * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new NRSN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 *
	 * This is useful for UNIONs, where we want to generate a DistinctNode above
	 * the UnionNode to eliminate the duplicates, because the type going into the
	 * sort has to agree with what the sort expects.
	 * (insert into t1 (smallintcol) values 1 union all values 2;
	 *
     * @param chldRes   The child ResultSetNode
     * @param targetResultColumnList The target resultColumnList from 
     *                          the InsertNode or UpdateNode. These will
     *                          be the types used for the NormalizeResultSetNode.
	 * @param tableProperties	Properties list associated with the table
	 * @param forUpdate 	tells us if the normalize operation is being
	 * performed on behalf of an update statement. 
     * @param cm                The context manager
	 * @throws StandardException 
	 */

    NormalizeResultSetNode(ResultSetNode chldRes,
                           ResultColumnList targetResultColumnList,
                           Properties tableProperties,
                           boolean forUpdate,
                           ContextManager cm) throws StandardException
	{
        super(chldRes, tableProperties, cm);
        this.forUpdate = forUpdate;

        ResultColumnList rcl = chldRes.getResultColumns();
        ResultColumnList targetRCL = targetResultColumnList;
        
		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 * 
		 * Setting this.resultColumns to the modified child result column list,
		 * and making a new copy for the child result set node
		 * ensures that the ProjectRestrictNode restrictions still points to 
		 * the same list.  See d3494_npe_writeup-4.html in DERBY-3494 for a
		 * detailed explanation of how this works.
		 */
		ResultColumnList prRCList = rcl;
        chldRes.setResultColumns(rcl.copyListAndObjects());
		// Remove any columns that were generated.
		prRCList.removeGeneratedGroupingColumns();
        // And also columns that were added for ORDER BY (DERBY-6006).
        prRCList.removeOrderByColumns();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the NormalizeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, rsn, and source ResultColumn.)
		 */
        prRCList.genVirtualColumnNodes(chldRes, chldRes.getResultColumns());
        
		setResultColumns( prRCList );
		// Propagate the referenced table map if it's already been created
        if (chldRes.getReferencedTableMap() != null)
		    {
			setReferencedTableMap((JBitSet) getReferencedTableMap().clone());
		    }
        
        
		if (targetResultColumnList != null) {
		    int size = Math.min(targetRCL.size(), getResultColumns().size());

            for (int index = 0; index < size; index++) {
                ResultColumn sourceRC = getResultColumns().elementAt(index);
                ResultColumn resultColumn = targetRCL.elementAt(index);
                sourceRC.setType(resultColumn.getTypeServices());
		    }
		}
	}


    /**
     *
	 *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		int				erdNumber;

		if (SanityManager.DEBUG)
            SanityManager.ASSERT(getResultColumns() != null, "Tree structure bad");

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		// Generate the child ResultSet

		// Get the cost estimate for the child
		setCostEstimate( childResult.getFinalCostEstimate() );

		erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);
		childResult.generate(acb, mb);
		mb.push(getResultSetNumber());
		mb.push(erdNumber);
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());
		mb.push(forUpdate);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 6);
	}

	/**
	 * set the Information gathered from the parent table that is 
     * required to perform a referential action on dependent table.
	 */
    @Override
    void setRefActionInfo(long fkIndexConglomId,
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		childResult.setRefActionInfo(fkIndexConglomId,
								   fkColArray,
								   parentResultSetId,
								   dependentScan);
	}

    @Override
    public void pushQueryExpressionSuffix() {
        childResult.pushQueryExpressionSuffix();
    }


	/**
	 * Push the order by list down from InsertNode into its child result set so
	 * that the optimizer has all of the information that it needs to consider
	 * sort avoidance.
	 *
	 * @param orderByList	The order by list
	 */
    @Override
	void pushOrderByList(OrderByList orderByList)
	{
		childResult.pushOrderByList(orderByList);
	}

    /**
     * Push through the offset and fetch first parameters, if any, to the child
     * result set.
     *
     * @param offset    the OFFSET, if any
     * @param fetchFirst the OFFSET FIRST, if any
     * @param hasJDBClimitClause true if the clauses were added by (and have the semantics of) a JDBC limit clause
     */
    @Override
    void pushOffsetFetchFirst( ValueNode offset, ValueNode fetchFirst, boolean hasJDBClimitClause )
    {
        childResult.pushOffsetFetchFirst( offset, fetchFirst, hasJDBClimitClause );
    }
}

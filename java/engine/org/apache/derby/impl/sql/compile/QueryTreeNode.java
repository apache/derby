/*

   Derby - Class org.apache.derby.impl.sql.compile.QueryTreeNode

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.execute.GenericConstantActionFactory;
import org.apache.derby.impl.sql.execute.GenericExecutionFactory;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.catalog.AliasInfo;
import java.util.Properties;
import java.util.Vector;
import java.sql.Types;
import org.apache.derby.iapi.reference.ClassName;

/**
 * QueryTreeNode is the root class for all query tree nodes. All
 * query tree nodes inherit from QueryTreeNode except for those that extend
 * QueryTreeNodeVector.
 *
 * @author Jeff Lichtman
 */

public abstract class QueryTreeNode implements Visitable
{
	public static final int AUTOINCREMENT_START_INDEX = 0;
	public static final int AUTOINCREMENT_INC_INDEX   = 1;
	public static final int AUTOINCREMENT_IS_AUTOINCREMENT_INDEX   = 2;

	int				beginOffset;		// offset into SQL input of the substring
	                                // which this query node encodes.
	int				endOffset;

	private int nodeType;
	private ContextManager cm;
	private LanguageConnectionContext lcc;
	private GenericConstantActionFactory	constantActionFactory;

	/**
	 * Set the ContextManager for this node.
	 * 
	 * @param cm	The ContextManager.
	 *
	 * @return Nothing.
	 */
	public void setContextManager(ContextManager cm)
	{
		this.cm = cm;
		
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(cm != null,
				"cm not expected to be null");
		}
	}

	/**
	 * Get the current ContextManager.
	 *
	 * @return The current ContextManager.
	 */
	public final ContextManager getContextManager()
	{
		if (SanityManager.DEBUG) {
			if (cm == null)
				SanityManager.THROWASSERT("Null context manager in QueryTreeNode of type :" + this.getClass());
		}
		return cm;
	}

	/**
	  *	Gets the NodeFactory for this database.
	  *
	  *	@return	the node factory for this database.
	  *
	  */
	public	final NodeFactory	getNodeFactory() 
	{
		return getLanguageConnectionContext().getLanguageConnectionFactory().
			                                            getNodeFactory();
	}


	/**
	  *	Gets the constant action factory for this database.
	  *
	  *	@return	the constant action factory.
	  */
	public	final GenericConstantActionFactory	getGenericConstantActionFactory()
	{
		if ( constantActionFactory == null )
		{
			GenericExecutionFactory	execFactory = (GenericExecutionFactory) getExecutionFactory();
			constantActionFactory = execFactory.getConstantActionFactory();
		}

		return constantActionFactory;
	}

	public	final	ExecutionFactory	getExecutionFactory()
	{
		ExecutionFactory	ef = getLanguageConnectionContext().getLanguageConnectionFactory().getExecutionFactory();

		return ef;
	}

	/**
		Get the ClassFactory to use with this database.
	*/
	protected final ClassFactory getClassFactory() {
		return getLanguageConnectionContext().getLanguageConnectionFactory().
			getClassFactory();
	}

	/**
	  *	Gets the LanguageConnectionContext for this connection.
	  *
	  *	@return	the lcc for this connection
	  *
	  */
	protected final LanguageConnectionContext	getLanguageConnectionContext()
	{
		if (lcc == null)
		{
			lcc = (LanguageConnectionContext) getContextManager().
							getContext(LanguageConnectionContext.CONTEXT_ID);
		}
		return lcc;
	}

	/**
	 * Get the name of the SPS that is used
	 * to execute this statement.  Only relevant
	 * for an ExecSPSNode -- otherwise, returns null.
	 *
	 * @return the name of the underlying sps
	 */
	public String getSPSName()
	{
		return null;
	}

	/**
	 * Gets the beginning offset of the SQL substring which this
	 * query node represents.
	 *
	 * @return	The beginning offset of the SQL substring.
	 *
	 */
    public	int	getBeginOffset() { return beginOffset; }

	/**
	 * Sets the beginning offset of the SQL substring which this
	 * query node represents.
	 *
	 * @param	The beginning offset of the SQL substring.
	 *
	 */
    public	void	setBeginOffset( int beginOffset )
	{
		this.beginOffset = beginOffset;
	}

	/**
	 * Gets the ending offset of the SQL substring which this
	 * query node represents.
	 *
	 * @return	The ending offset of the SQL substring.
	 *
	 */
	public	int	getEndOffset()  { return endOffset; }

	/**
	 * Sets the ending offset of the SQL substring which this
	 * query node represents.
	 *
	 * @param	The ending offset of the SQL substring.
	 *
	 */
	public	void	setEndOffset( int endOffset )
	{
		this.endOffset = endOffset;
	}


	/**
	 * Return header information for debug printing of this query
	 * tree node.
	 *
	 * @return	Header information for debug printing of this query
	 *		tree node.
	 */

	protected String	nodeHeader()
	{
		if (SanityManager.DEBUG)
		{
			return "\n" + this.getClass().getName() + '@' +
					Integer.toHexString(hashCode()) + "\n";
		}
		else
		{
			return "";
		}
	}

	/**
	 * Format a node that has been converted to a String for printing
	 * as part of a tree.  This method indents the String to the given
	 * depth by inserting tabs at the beginning of the string, and also
	 * after every newline.
	 *
	 * @param nodeString	The node formatted as a String
	 * @param depth		The depth to indent the given node
	 *
	 * @return	The node String reformatted with tab indentation
	 */

	public static String formatNodeString(String nodeString, int depth)
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer	nodeStringBuffer = new StringBuffer(nodeString);
			int		pos;
			char		c;
			char[]		indent = new char[depth];

			/*
			** Form an array of tab characters for indentation.
			*/
			while (depth > 0)
			{
				indent[depth - 1] = '\t';
				depth--;
			}

			/* Indent the beginning of the string */
			nodeStringBuffer.insert(0, indent);

			/*
			** Look for newline characters, except for the last character.
			** We don't want to indent after the last newline.
			*/
			for (pos = 0; pos < nodeStringBuffer.length() - 1; pos++)
			{
				c = nodeStringBuffer.charAt(pos);
				if (c == '\n')
				{
					/* Indent again after each newline */
					nodeStringBuffer.insert(pos + 1, indent);
				}
			}

			return nodeStringBuffer.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Print this tree for debugging purposes.  This recurses through
	 * all the sub-nodes and prints them indented by their depth in
	 * the tree.
	 *
	 * @return	Nothing
	 */

	public void treePrint()
	{
		if (SanityManager.DEBUG)
		{
			debugPrint(nodeHeader());
			debugPrint(formatNodeString(this.toString(), 0));
			printSubNodes(0);
			debugFlush();
		}
	}

	/**
	 * Print this tree for debugging purposes.  This recurses through
	 * all the sub-nodes and prints them indented by their depth in
	 * the tree, starting with the given indentation.
	 *
	 * @param depth		The depth of this node in the tree, thus,
	 *			the amount to indent it when printing it.
	 *
	 * @return	Nothing
	 */

	public void treePrint(int depth)
	{
		if (SanityManager.DEBUG)
		{
			debugPrint(formatNodeString(nodeHeader(), depth));
			debugPrint(formatNodeString(this.toString(), depth));
			printSubNodes(depth);
		}
	}

	/**
	 * Print a String for debugging
	 *
	 * @param outputString	The String to print
	 *
	 * @return	Nothing
	 */

	public static void debugPrint(String outputString)
	{
		if (SanityManager.DEBUG) {
			SanityManager.GET_DEBUG_STREAM().print(outputString);
		}
	}

	/**
	 * Flush the debug stream out
	 *
	 * @return	Nothing
	 */
	protected static void debugFlush()
	{
		if (SanityManager.DEBUG) {
			SanityManager.GET_DEBUG_STREAM().flush();
		}
	}

	/**
	 * Print the sub-nodes of this node.
	 *
	 * Each sub-class of QueryTreeNode is expected to provide its own
	 * printSubNodes() method.  In each case, it calls super.printSubNodes(),
	 * passing along its depth, to get the sub-nodes of the super-class.
	 * Then it prints its own sub-nodes by calling treePrint() on each
	 * of its members that is a type of QueryTreeNode.  In each case where
	 * it calls treePrint(), it should pass "depth + 1" to indicate that
	 * the sub-node should be indented one more level when printing.
	 * Also, it should call printLabel() to print the name of each sub-node
	 * before calling treePrint() on the sub-node, so that the reader of
	 * the printed tree can tell what the sub-node is.
	 *
	 * This printSubNodes() exists in here merely to act as a backstop.
	 * In other words, the calls to printSubNodes() move up the type
	 * hierarchy, and in this node the calls stop.
	 *
	 * I would have liked to put the call to super.printSubNodes() in
	 * this super-class, but Java resolves "super" statically, so it
	 * wouldn't get to the right super-class.
	 *
	 * @param depth		The depth to indent the sub-nodes
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
	}

	/**
	 * Format this node as a string
	 *
	 * Each sub-class of QueryTreeNode should implement its own toString()
	 * method.  In each case, toString() should format the class members
	 * that are not sub-types of QueryTreeNode (printSubNodes() takes care
	 * of following the references to sub-nodes, and toString() takes care
	 * of all members that are not sub-nodes).  Newlines should be used
	 * liberally - one good way to do this is to have a newline at the
	 * end of each formatted member.  It's also a good idea to put the
	 * name of each member in front of the formatted value.  For example,
	 * the code might look like:
	 *
	 * "memberName: " + memberName + "\n" + ...
	 *
	 * @return	This node formatted as a String
	 */

	public String toString()
	{
		return "";
	}

	/**
	 * Print the given label at the given indentation depth.
	 *
	 * @param depth		The depth of indentation to use when printing
	 *			the label
	 * @param label		The String to print
	 *
	 * @return	Nothing
	 */

	public void printLabel(int depth, String label)
	{
		if (SanityManager.DEBUG)
		{
			debugPrint(formatNodeString(label, depth));
		}
	}

	/**
	 * Perform the binding operation on a query tree.  Binding consists of
	 * permissions checking, view resolution, datatype resolution, and
	 * creation of a dependency list (for determining whether a tree or
	 * plan is still up to date).
	 *
	 * This bind() method does nothing.  Each node type that can appear
	 * at the top of a tree can override this method with its own bind()
	 * method that does "something".
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		return this;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		return false;
	}

	/**
	 * Checks if the passed schema descriptor is for SESSION schema
	 *
	 * @return	true if the passed schema descriptor is for SESSION schema
	 *
	 * @exception StandardException		Thrown on error
	 */
	final boolean isSessionSchema(SchemaDescriptor sd)
	{
		return isSessionSchema(sd.getSchemaName());
	}

	/**
	 * Checks if the passed schema name is for SESSION schema
	 *
	 * @return	true if the passed schema name is for SESSION schema
	 *
	 * @exception StandardException		Thrown on error
	 */
	final boolean isSessionSchema(String schemaName)
	{
		return SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME.equals(schemaName);
	}

	/**
	 * Get the optimizer's estimate of the number of rows returned or affected
	 * for an optimized QueryTree.
	 *
	 * For non-optimizable statements (for example, CREATE TABLE),
	 * return 0. For optimizable statements, this method will be
	 * over-ridden in the statement's root node (DMLStatementNode
	 * in all cases we know about so far).
	 *
	 * @return	0L
	 */

	public long	getRowEstimate()
	{
		return	0L;
	}

	/**
	 * Generates an optimized QueryTree from a bound QueryTree.  Actually,
	 * it annotates the tree in place rather than generating a new tree,
	 * but this interface allows the root node of the optmized QueryTree
	 * to be different from the root node of the bound QueryTree.
	 *
	 * For non-optimizable statements (for example, CREATE TABLE),
	 * return the bound tree without doing anything.  For optimizable
	 * statements, this method will be over-ridden in the statement's
	 * root node (DMLStatementNode in all cases we know about so far).
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 * @return	An optimized QueryTree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode optimize() throws StandardException
	{
		return this;
	}

	/**
	 * this implementation of generate() is
	 * a place-holder until all of the nodes that need to,
	 * implement it. Only the root, statement nodes
	 * implement this flavor of generate; the other nodes
	 * will implement the version that returns Generators
	 * and takes an activation class builder as an
	 * argument.
	 *
	 * @param	ignored - ignored (he he)
	 *
	 * @return	A GeneratedClass for this statement
	 *
	 * @exception StandardException		Thrown on error
	 */
	public GeneratedClass generate(ByteArray ignored) throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNABLE_TO_GENERATE,
			this.nodeHeader());
	}

	/**
	 * Do the code generation for this node.  This is a place-holder
	 * method - it should be over-ridden in the sub-classes.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the generated code to go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected void generate(
								ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNABLE_TO_GENERATE,
			this.nodeHeader());
	}

	/**
	 * Only DML statements have result descriptions - for all others
	 * return null.  This method is overridden in DMLStatementNode.
	 *
	 * @return	null
	 *
	 * @exception StandardException never actually thrown here,
	 *	but thrown by subclasses
	 */
	public ResultDescription makeResultDescription() 
		throws StandardException
	{
		return null;
	}

	/**
	 * Parameter info is stored in the compiler context.
	 * Hide this from the callers.
	 *
	 *
	 * @return	null
	 *
	 * @exception StandardException on error
	 */
	public DataTypeDescriptor[] getParameterTypes()
		throws StandardException
	{
		return getCompilerContext().getParameterTypes();
	}

	/**
	 * This creates a class that will do the work that's constant
	 * across all Executions of a PreparedStatement. It's up to
	 * our subclasses to override this method if they need to compile
	 * constant actions into PreparedStatements.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	null;
	}

	/**
	 * Returns whether or not this Statement requires a set/clear savepoint
	 * around its execution.  The following statement "types" do not require them:
	 *		Cursor	- unnecessary and won't work in a read only environment
	 *		Xact	- savepoint will get blown away underneath us during commit/rollback
	 * <p>
	 * ONLY CALLABLE AFTER GENERATION
	 *
	 * @return boolean	Whether or not this Statement requires a set/clear savepoint
	 */
	public boolean needsSavepoint()
	{
		return true;
	}

	/**
	 * Returns the name of statement in EXECUTE STATEMENT command.
	 * Returns null for all other commands.  
	 * @return String null unless overridden for Execute Statement command
	 */
	public String executeStatementName()
	{
		return null;
	}

  /**
   * Returns name of schema in EXECUTE STATEMENT command.
   * Returns null for all other commands.
   * @return String schema for EXECUTE STATEMENT null for all others
   */
	public String executeSchemaName()
	{
		return null;
	}

    /**
	 * Set the node type for this node.
	 *
	 * @param nodeType The node type.
	 *
	 * @return Nothing.
	 */
	public void setNodeType(int nodeType)
	{
		this.nodeType = nodeType;
	}

	protected int getNodeType()
	{
		return nodeType;
	}

	/**
	 * For final nodes, return whether or not
	 * the node represents the specified nodeType.
	 *
	 * @param nodeType	The nodeType of interest.
	 *
	 * @return Whether or not
	 * the node represents the specified nodeType.
	 */
	protected boolean isInstanceOf(int nodeType)
	{
		return (this.nodeType == nodeType);
	}

	/**
	 * Get the DataDictionary
	 *
	 * @return The DataDictionary
	 *
	 */
	public final DataDictionary getDataDictionary()
	{
		return getLanguageConnectionContext().getDataDictionary();
	}

	public final DependencyManager getDependencyManager()
	{
		return getDataDictionary().getDependencyManager();
	}

	/**
	 * Get the CompilerContext
	 *
	 * @return The CompilerContext
	 */
	protected final CompilerContext getCompilerContext()
	{
		return (CompilerContext) getContextManager().
										getContext(CompilerContext.CONTEXT_ID);
	}

	/**
	 * Get the TypeCompiler associated with the given TypeId
	 *
	 * @param typeId	The TypeId to get a TypeCompiler for
	 *
	 * @return	The corresponding TypeCompiler
	 *
	 */
	protected final TypeCompiler getTypeCompiler(TypeId typeId)
	{
		return
		  getCompilerContext().getTypeCompilerFactory().getTypeCompiler(typeId);
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		return v.visit(this);
	}

	/**
	 * Get the int value of a Property
	 *
	 * @param value		Property value as a String
	 * @param key		Key value of property
	 *
	 * @return	The int value of the property
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected int getIntProperty(String value, String key)
		throws StandardException
	{
		int intVal = -1;
		try
		{
			intVal = Integer.parseInt(value);
		}
		catch (NumberFormatException nfe)
		{
			throw StandardException.newException(SQLState.LANG_INVALID_NUMBER_FORMAT_FOR_OVERRIDE, 
					value, key);
		}
		return intVal;
	}

	/**
	 * Parse some query text and return a parse tree.
	 *
	 * @param compilerContext	The CompilerContext to use
	 * @param createViewText	Query text to parse.
	 * @param paramDefaults		array of parameter defaults used to
	 *							initialize parameter nodes, and ultimately
	 *							for the optimization of statements with
	 *							parameters.
	 * @param lcc				Current LanguageConnectionContext
	 *
	 * @return	ResultSetNode	The parse tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static QueryTreeNode
	parseQueryText
	(
		CompilerContext		compilerContext,
		String				queryText,
		Object[]			paramDefaults,
		LanguageConnectionContext lcc
    )
		 throws StandardException
	{
		LanguageConnectionFactory	lcf;
		Parser						p;
		QueryTreeNode			    qtn;

		p = compilerContext.getParser();
		
		/* Get a Statement to pass to the parser */
		lcf = lcc.getLanguageConnectionFactory();

		/* Finally, we can call the parser */
		qtn = (QueryTreeNode)p.parseStatement(queryText, paramDefaults);
		return	qtn;
	}

	/**
	 * Return the type of statement, something from
	 * StatementType.
	 *
	 * @return the type of statement
	 */
	protected int getStatementType()
	{
		return StatementType.UNKNOWN;
	}

	public boolean foundString(String[] list, String search)
	{
		if (list == null)
		{
			return false;
		}

		for (int i = 0; i < list.length; i++)
		{
			if (list[i].equals(search))
			{	
				return true;
			}
		}
		return false;
	}

	/**
	 * Get a ConstantNode to represent a typed null value
	 *
	 * @param typeId	The TypeId of the datatype of the null value
	 * @param cm		The ContextManager
	 *
	 * @return	A ConstantNode with the specified type, and a value of null
	 *
	 * @exception StandardException		Thrown on error
	 */
	public  ConstantNode getNullNode(TypeId typeId,
			ContextManager cm)
		throws StandardException
	{
		QueryTreeNode constantNode = null;
		NodeFactory nf = getNodeFactory();

		switch (typeId.getJDBCTypeId())
		{
		  case Types.VARCHAR:
			constantNode =  nf.getNode(
										C_NodeTypes.VARCHAR_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.CHAR:
			constantNode = nf.getNode(
										C_NodeTypes.CHAR_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.TINYINT:
			constantNode = nf.getNode(
										C_NodeTypes.TINYINT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.SMALLINT:
			constantNode = nf.getNode(
										C_NodeTypes.SMALLINT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.INTEGER:
			constantNode = nf.getNode(
										C_NodeTypes.INT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.BIGINT:
			constantNode = nf.getNode(
										C_NodeTypes.LONGINT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.REAL:
			constantNode = nf.getNode(
										C_NodeTypes.FLOAT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.DOUBLE:
			constantNode = nf.getNode(
										C_NodeTypes.DOUBLE_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.NUMERIC:
		  case Types.DECIMAL:
			constantNode = nf.getNode(
										C_NodeTypes.DECIMAL_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.DATE:
		  case Types.TIME:
		  case Types.TIMESTAMP:
			constantNode = nf.getNode(
										C_NodeTypes.USERTYPE_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.BINARY:
			constantNode = nf.getNode(
										C_NodeTypes.BIT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.VARBINARY:
			constantNode = nf.getNode(
										C_NodeTypes.VARBIT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.LONGVARCHAR:
			constantNode = nf.getNode(
										C_NodeTypes.LONGVARCHAR_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.CLOB:
			constantNode = nf.getNode(
										C_NodeTypes.CLOB_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.LONGVARBINARY:
			constantNode = nf.getNode(
										C_NodeTypes.LONGVARBIT_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  case Types.BLOB:
			constantNode = nf.getNode(
										C_NodeTypes.BLOB_CONSTANT_NODE,
										typeId,
										cm);
			break;

		  default:
			if (typeId.getSQLTypeName().equals("BOOLEAN"))
			{
				constantNode = nf.getNode(
										C_NodeTypes.BOOLEAN_CONSTANT_NODE,
										typeId,
										cm);
			}
			else if ( ! typeId.builtIn())
			{
				constantNode = nf.getNode(
										C_NodeTypes.USERTYPE_CONSTANT_NODE,
										typeId,
										cm);
			}
			else
			{
				if (SanityManager.DEBUG)
				SanityManager.THROWASSERT( "Unknown type " + 
						typeId.getSQLTypeName() + " in getNullNode");
				return null;
			}
		}

		return (ConstantNode) constantNode;
	}

	/**
	 * Translate a Default node into a default value, given a type descriptor.
	 *
	 * @param typeDescriptor	A description of the required data type.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor convertDefaultNode(DataTypeDescriptor typeDescriptor)
							throws StandardException
	{
		/*
		** Override in cases where node type
		** can be converted to default value.
		*/
		return null;
	}

	/* Initializable methods */

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Single-argument init() not implemented for " + getClass().getName());
		}
	}


	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Two-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Three-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Four-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Five-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Six-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Seven-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Eight-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Nine-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9,
						Object arg10) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Ten-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9,
						Object arg10,
						Object arg11) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Eleven-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9,
						Object arg10,
						Object arg11,
						Object arg12) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Twelve-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9,
						Object arg10,
						Object arg11,
						Object arg12,
						Object arg13) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Thirteen-argument init() not implemented for " + getClass().getName());
		}
	}

	/**
	 * Initialize a query tree node.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object arg1,
						Object arg2,
						Object arg3,
						Object arg4,
						Object arg5,
						Object arg6,
						Object arg7,
						Object arg8,
						Object arg9,
						Object arg10,
						Object arg11,
						Object arg12,
						Object arg13,
						Object arg14) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("Fourteen-argument init() not implemented for " + getClass().getName());
		}
	}

	public	TableName	makeTableName
	(
		String	schemaName,
		String	flatName
	)
		throws StandardException
	{
		return (TableName) getNodeFactory().getNode
			(
				C_NodeTypes.TABLE_NAME,
				schemaName,
				flatName,
				getContextManager()
			);
	}

	public boolean isAtomic() throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("isAtomic should not be called for this  class: " + getClass().getName());
		}
		
		return false;
	}
	
	public Object getCursorInfo() throws StandardException
	{
		return null;
	}

	/**
	 * Get the descriptor for the named table within the given schema.
	 * If the schema parameter is NULL, it looks for the table in the
	 * current (default) schema. Table descriptors include object ids,
	 * object types (table, view, etc.)
	 * If the schema is SESSION, then before looking into the data dictionary
	 * for persistent tables, it first looks into LCC for temporary tables.
	 * If no temporary table tableName found for the SESSION schema, then it goes and
	 * looks through the data dictionary for persistent table
	 * We added getTableDescriptor here so that we can look for non data dictionary
	 * tables(ie temp tables) here. Any calls to getTableDescriptor in data dictionary
	 * should be only for persistent tables
	 *
	 * @param tableName	The name of the table to get the descriptor for
	 * @param schema	The descriptor for the schema the table lives in.
	 *			If null, use the current (default) schema.
	 *
	 * @return	The descriptor for the table, null if table does not
	 *		exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected final TableDescriptor getTableDescriptor(String tableName,
					SchemaDescriptor schema)
						throws StandardException
	{
		TableDescriptor retval;

		//Following if means we are dealing with SESSION schema.
		if (isSessionSchema(schema))
		{
			//First we need to look in the list of temporary tables to see if this table is a temporary table.
			retval = getLanguageConnectionContext().getTableDescriptorForDeclaredGlobalTempTable(tableName);
			if (retval != null)
				return retval; //this is a temporary table
		}

		//Following if means we are dealing with SESSION schema and we are dealing with in-memory schema (ie there is no physical SESSION schema)
		//If following if is true, it means SESSION.table is not a declared table & it can't be physical SESSION.table
		//because there is no physical SESSION schema
		if (schema.getUUID() == null)
			return null;

		//it is not a temporary table, so go through the data dictionary to find the physical persistent table
		return getDataDictionary().getTableDescriptor(tableName, schema);
	}

	/**
	 * Get the descriptor for the named schema. If the schemaName
	 * parameter is NULL, it gets the descriptor for the current (default)
	 * schema. Schema descriptors include authorization ids and schema ids.
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.  Will check default schema for a match
	 * before scanning a system table.
	 * 
	 * @param schemaName	The name of the schema we're interested in.
	 *			If the name is NULL, get the descriptor for the
	 *			current schema.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on error
	 */
	final SchemaDescriptor	getSchemaDescriptor(String schemaName)
		throws StandardException
	{
		//return getSchemaDescriptor(schemaName, schemaName != null);
		return getSchemaDescriptor(schemaName, true);
	}
	final SchemaDescriptor	getSchemaDescriptor(String schemaName, boolean raiseError)
		throws StandardException
	{
		/*
		** Check for a compilation context.  Sometimes
		** there is a special compilation context in
	 	** place to recompile something that may have
		** been compiled against a different schema than
		** the current schema (e.g views):
	 	**
	 	** 	CREATE SCHEMA x
	 	** 	CREATE TABLE t
		** 	CREATE VIEW vt as SEELCT * FROM t
		** 	SET SCHEMA app
		** 	SELECT * FROM X.vt 
		**
		** In the above view vt must be compiled against
		** the X schema.
		*/


		SchemaDescriptor sd = null;
		boolean isCurrent = false;
		boolean isCompilation = false;
		if (schemaName == null) {

			CompilerContext cc = getCompilerContext();
			sd = cc.getCompilationSchema();

			if (sd == null) {
				// Set the compilation schema to be the default,
				// notes that this query has schema dependencies.
				sd = getLanguageConnectionContext().getDefaultSchema();

				isCurrent = true;

				cc.setCompilationSchema(sd);
			}
			else
			{
				isCompilation = true;
			}
			schemaName = sd.getSchemaName();
		}

		DataDictionary dataDictionary = getDataDictionary();
		SchemaDescriptor sdCatalog = dataDictionary.getSchemaDescriptor(schemaName,
			getLanguageConnectionContext().getTransactionCompile(), raiseError);

		if (isCurrent || isCompilation) {
			if (sdCatalog != null)
			{
				// different UUID for default (current) schema than in catalog,
				// so reset default schema.
				if (!sdCatalog.getUUID().equals(sd.getUUID()))
				{
					if (isCurrent)
						getLanguageConnectionContext().setDefaultSchema(sdCatalog);
					getCompilerContext().setCompilationSchema(sdCatalog);
				}
			}
			else
			{
				// this schema does not exist, so ensure its UUID is null.
				sd.setUUID(null);
				sdCatalog = sd;
			}
		}
		return sdCatalog;
	}

	/**
	 * 
	 * @param javaClassName	The name of the java class to resolve.
	 *
	 * @param convertCase	whether to convert the case before resolving class alias.
	 *
	 * @return	Resolved class name or class alias name.
	 *
	 * @exception StandardException		Thrown on error
	 */
	String verifyClassExist(String javaClassName, boolean convertCase)
		throws StandardException
	{
		/* Verify that the class exists */

		ClassInspector classInspector = getClassFactory().getClassInspector();

		/* We first try to resolve the javaClassName as a class.  If that
		 * fails then we try to resolve it as a class alias.
		 */

		Throwable reason = null;
		boolean foundMatch = false;
		try {

			foundMatch = classInspector.accessible(javaClassName);

		} catch (ClassNotFoundException cnfe) {

			reason = cnfe;
		} catch (LinkageError le) {
			reason = le;
		}

		if (!foundMatch)
			throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2, reason, javaClassName);

		if (ClassInspector.primitiveType(javaClassName))
			throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST3, javaClassName);

		return javaClassName;
	}

	/**
	 * set the Information gathered from the parent table that is 
	 * required to peform a referential action on dependent table.
	 *
	 * @return Nothing.
	 */
	public void setRefActionInfo(long fkIndexConglomId, 
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"setRefActionInfo() not expected to be called for " +
				getClass().getName());
		}
	}

	/**
		Add an authorization check into the passed in method.
	*/
	void generateAuthorizeCheck(ActivationClassBuilder acb,
								MethodBuilder mb,
								int sqlOperation) {
		// add code to authorize statement execution.
		acb.pushThisAsActivation(mb);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getLanguageConnectionContext",
											 ClassName.LanguageConnectionContext, 0);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getAuthorizer",
											 ClassName.Authorizer, 0);

		mb.push(sqlOperation);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "authorize",
											 "void", 1);
	}
	

}












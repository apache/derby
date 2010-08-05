/**
 * This class will perform the database connection establishment,
 * querying the database, shut downing the database.
 * Created under DERBY-4587-PlanExporter tool
 */

package org.apache.derby.impl.tools.planexporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author Nirmal
 *
 */
public class AccessDatabase {

	private Connection conn = null;
	private Statement stmt = null;
	private TreeNode[] data;
	private String dbURL = null;
	private String schema = null;
	private String query = null;
	private int depth = 0;	
	public int getDepth() {
		return depth;
	}
	private String xmlDetails="";

	//set of variables to identify values of XPlain tables
	private final int id =0 ;
	private final int p_id =1;
	private final int nodeType=2;
	private final int noOfOpens=3;
	private final int inputRows=4;
	private final int returnedRows=5;
	private final int visitedPages=6;
	private final int scanQualifiers=7;
	private final int nextQualifiers=8;
	private final int scannedObject=9;
	private final int scanType=10;
	private final int sortType=11;
	private final int noOfOutputRowsBySorter=12;


	/**
	 * 
	 * @param dburl
	 * @param aSchema
	 * @param aQuery
	 */
	public AccessDatabase(String dburl, String aSchema, String aQuery) {

		dbURL = dburl;
		schema = aSchema;
		query = aQuery;

	}

	/**
	 * 
	 * @param aConn
	 * @param aSchema
	 * @param aQuery
	 */
	public AccessDatabase(Connection aConn, String aSchema, String aQuery) {

		conn = aConn;
		schema = aSchema;
		query = aQuery;

	}

	/**
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException 
	 */
	public void createConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{

		if(dbURL.indexOf("//") != -1)
			Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

		else
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();

		//Get a connection
		conn = DriverManager.getConnection(dbURL); 

	}

	/**
	 * <p>
	 * This method creates the queries such that after execution
	 * of the query it will return XML data fragments.
	 * </P>
	 * @throws SQLException
	 * */
	public void createXMLFragment() throws SQLException{
		createXMLData(
				"select 'id=\"' ||RS_ID|| '\"' " +
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", id);

		createXMLData(
				"select PARENT_RS_ID "+
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", p_id);

		createXMLData(
				"select 'name=\"' ||OP_IDENTIFIER|| '\"' " +
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", nodeType);

		createXMLData(
				"select 'no_opens=\"' " +
				"|| TRIM(CHAR(NO_OPENS))|| '\"' " +
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", noOfOpens);

		createXMLData(
				"select 'input_rows=\"' " +
				"|| TRIM(CHAR(INPUT_ROWS))|| '\"' " +
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", inputRows);

		createXMLData(
				"select 'returned_rows=\"' " +
				"|| TRIM(CHAR(RETURNED_ROWS))|| '\"' " +
				"from "+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'", returnedRows);

		createXMLData(
				"select 'visited_pages=\"'" +
				"|| TRIM(CHAR(NO_VISITED_PAGES))|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", visitedPages);

		createXMLData(
				"select 'scan_qualifiers=\"'"+ 
				"||SCAN_QUALIFIERS|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", scanQualifiers);

		createXMLData(
				"select 'next_qualifiers=\"'"+
				"||NEXT_QUALIFIERS|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", nextQualifiers);

		createXMLData(
				"select 'scanned_object=\"'"+
				"||SCAN_OBJECT_NAME|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", scannedObject);

		createXMLData(
				"select 'scan_type=\"'"+
				"||TRIM(SCAN_TYPE)|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", scanType);

		createXMLData(
				"select 'sort_type=\"'"+
				"||TRIM(SORT_TYPE)|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SORT_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", sortType);

		createXMLData(
				"select 'sorter_output=\"'"+
				"||TRIM(CHAR(NO_OUTPUT_ROWS))|| '\"' " +
				"from ("+schema+".SYSXPLAIN_SORT_PROPS " +
				"NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
				"where STMT_ID = '"+query+"'", noOfOutputRowsBySorter);

	}

	/**
	 * 
	 * @return all xml elements as a String
	 */
	public String getXmlString(){

		for(int i=0;i<data.length;i++){
			//assume only one root element for any query
			if(Integer.parseInt(data[i].getDepth())==0){//root element
				xmlDetails += indent(1);
				xmlDetails += data[i].toString();
				getChildren(1, data[i].getId());
				xmlDetails += indent(1)+"</node>\n";
				break;
			}
		}
		return xmlDetails;
	}

	/**
	 * 
	 * @param currentLevel level of the XML tree (0 based) of current node
	 * @param id current node's stmt_id
	 */
	public void getChildren(int currentLevel,String id ){
		if(currentLevel <= depth){
			for(int i=0;i<data.length;i++){
				if(Integer.parseInt(data[i].getDepth())== currentLevel &&
						(id.indexOf(data[i].getParent()) != -1))
				{
					xmlDetails += indent(currentLevel+1);
					xmlDetails += data[i].toString();
					getChildren(currentLevel+1, data[i].getId());
					xmlDetails += indent(currentLevel+1)+"</node>\n";
				}
			}
		}
	}

	/**
	 * 
	 * @param j indent needed
	 * @return indent as a string
	 */
	public String indent(int j){
		String str="";
		for(int i=0;i<=j+1;i++)
			str +="	";

		return str;
	}

	/**
	 * marking the depth of each element
	 */
	public void markTheDepth(){
		int i=0;
		while(data[i].getParent().indexOf("null")== -1)
			i++;
		data[i].setDepth(""+depth); //root
		findChildren(i,depth);
	}

	/**
	 * 
	 * @param idx current element's index
	 * @param dep current examining depth
	 */
	public void findChildren(int idx, int dep) {
		if(dep>depth)
			depth =dep;

		for(int i=0;i<data.length;i++){

			if(data[i].getParent().indexOf("null")== -1){ 
				if((data[idx].getId()).indexOf(data[i].getParent()) != -1
						&& i != idx)
				{
					data[i].setDepth(""+(dep +1));
					findChildren(i,dep+1);	 
				}		
			}
		}
	}


	/**
	 * 
	 * @return whether the initialization is successful or not
	 * @throws SQLException 
	 */
	public boolean initializeDataArray() throws SQLException{
		if(noOfNodes()==0)
			return false;
		else{
			data = new TreeNode[noOfNodes()];
			for(int i=0; i<data.length;i++){
				data[i] = new TreeNode();
			}
			return true;
		}

	}

	/**
	 * 
	 * @param qry query to be executed
	 * @throws SQLException 
	 */
	private void createXMLData(String qry, int x) throws SQLException{

		stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery(qry);

		int i=0;
		while(results.next())
		{
			String text= results.getString(1);
			if(text != null){
				switch(x){
				case id: 
					data[i].setId(text+" ");
					break;
				case p_id:
					data[i].setParent(text);
					break;
				case nodeType:
					data[i].setNodeType(text+" ");
					break;
				case noOfOpens:
					data[i].setNoOfOpens(text+" ");
					break;
				case inputRows:
					data[i].setInputRows(text+" ");
					break;
				case returnedRows:
					data[i].setReturnedRows(text+" ");
					break;
				case visitedPages:
					data[i].setVisitedPages(text+" ");
					break;	
				case scanQualifiers:
					data[i].setScanQualifiers(text+" ");
					break;
				case nextQualifiers:
					data[i].setNextQualifiers(text+" ");
					break;
				case scannedObject:
					data[i].setScannedObject(text+" ");
					break;
				case scanType:
					data[i].setScanType(text+" ");
					break;
				case sortType:
					data[i].setSortType(text+" ");
					break;
				case noOfOutputRowsBySorter:
					data[i].setSorterOutput(text+" ");
					break;
				}
			}
			else{
				/*Other attributes are omitted from the xml document 
				 * if they're null.
				 * p_id can be null at the root.
				 * */
				switch(x){
				case p_id:
					data[i].setParent(text+"");
					break;
				}
			}
			i++;	 
		}
		results.close();
		stmt.close();
	}

	/**
	 * 
	 * @return total # of nodes
	 * @throws SQLException 
	 */
	public int noOfNodes() throws SQLException{

		stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery(
				"select count(*) from " +
				""+schema+".SYSXPLAIN_RESULTSETS " +
				"where STMT_ID = '"+query+"'");
		results.next();
		int no = results.getInt(1);
		results.close();
		stmt.close();
		return no;
	}


	/**
	 * 
	 * @return the <statement> element
	 * @throws SQLException 
	 */
	public String statement() throws SQLException{
		stmt = conn.createStatement();
		ResultSet results = stmt.executeQuery(
				"select STMT_TEXT "+
				"from "+schema+".SYSXPLAIN_STATEMENTS " +
				"where STMT_ID = '"+query+"'");
		results.next();
		String statement = results.getString(1);
		results.close();
		stmt.close();
		/*Removing possible less than and greater than characters
		 * in a query statement with XML representation.*/
		if(statement.indexOf('<')!= -1){
			statement = replace(statement, "<","&lt;");
		}
		if(statement.indexOf('>')!= -1){
			statement = replace(statement, ">","&gt;");
		}
		return "<statement>"+statement+"</statement>\n";
	}

	/**
	 * 
	 * @param stmt statement to be changed
	 * @param expr string to be removed
	 * @param replace string to be added
	 * @return modified string
	 */
	public String replace(String stmt, String expr, String replace){
		String[] part=stmt.split(expr);
		String newStmt= part[0];
		for(int i=1;i<part.length;i++){
			newStmt += " "+replace+" "+part[i];
		}

		return newStmt;
	}

	/**
	 * shut downing the connection to the database
	 */
	public void shutdown()
	{
		try
		{
			if (stmt != null)
			{
				stmt.close();
			}
			if (conn != null)
			{
				DriverManager.getConnection(dbURL + ";shutdown=true");
				conn.close();
			}           
		}
		catch (SQLException sqlExcept){}
	}

	/**
	 * 
	 * @return data array of TreeNode Objects 
	 */
	public TreeNode[] getData() {
		return data;
	}
}

/*

   Derby - Class org.apache.derby.impl.tools.planexporter.AccessDatabase

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.tools.planexporter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class will perform the database connection establishment,
 * querying the database, shut downing the database.
 * Created under DERBY-4587-PlanExporter tool
 */
public class AccessDatabase {

    private Connection conn = null;
    private Statement stmt = null;
    private TreeNode[] data;
    private String dbURL = null;
    private String schema = null;
    private String query = null;
    /**
     * @param query the stmt_id to set
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * @return the stmt_id
     */
    public String getQuery() {
        return query;
    }
    private int depth = 0;
    public int getDepth() {
        return depth;
    }
    private String xmlDetails="";

    //set of variables to identify values of XPlain tables
    private static final int ID =0 ;
    private static final int P_ID =1;
    private static final int NODE_TYPE=2;
    private static final int NO_OF_OPENS=3;
    private static final int INPUT_ROWS=4;
    private static final int RETURNED_ROWS=5;
    private static final int VISITED_PAGES=6;
    private static final int SCAN_QUALIFIERS=7;
    private static final int NEXT_QUALIFIERS=8;
    private static final int SCANNED_OBJECT=9;
    private static final int SCAN_TYPE=10;
    private static final int SORT_TYPE=11;
    private static final int NO_OF_OUTPUT_ROWS_BY_SORTER=12;


    /**
     *
     * @param dburl
     * @param aSchema
     * @param aQuery
     */
    public AccessDatabase(String dburl, String aSchema, String aQuery) {

        dbURL = dburl;
        schema = aSchema;
        setQuery(aQuery);

    }

    /**
     *
     * @param aConn
     * @param aSchema
     * @param aQuery
     *
     */
    public AccessDatabase(Connection aConn, String aSchema, String aQuery) {

        conn = aConn;
        schema = aSchema;
        setQuery(aQuery);

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

        if(dbURL.indexOf("://") != -1)
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

        else
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();

        //Get a connection
        conn = DriverManager.getConnection(dbURL);

    }

    public boolean verifySchemaExistance() throws SQLException{
    	boolean found=false;
    	ResultSet result = conn.getMetaData().getSchemas();
    	while(result.next()){
    		if(result.getString(1).equals(schema)){
    			found=true;
    			break;
    		}
    	}	
    	return found;
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
                "where STMT_ID = '"+getQuery()+"'", ID);

        createXMLData(
                "select PARENT_RS_ID "+
                "from "+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'", P_ID);

        createXMLData(
                "select 'name=\"' ||OP_IDENTIFIER|| '\"' " +
                "from "+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'", NODE_TYPE);

        createXMLData(
                "select 'no_opens=\"' " +
                "|| TRIM(CHAR(NO_OPENS))|| '\"' " +
                "from "+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'", NO_OF_OPENS);

        createXMLData(
                "select 'input_rows=\"' " +
                "|| TRIM(CHAR(INPUT_ROWS))|| '\"' " +
                "from "+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'", INPUT_ROWS);

        createXMLData(
                "select 'returned_rows=\"' " +
                "|| TRIM(CHAR(RETURNED_ROWS))|| '\"' " +
                "from "+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'", RETURNED_ROWS);

        createXMLData(
                "select 'visited_pages=\"'" +
                "|| TRIM(CHAR(NO_VISITED_PAGES))|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", VISITED_PAGES);

        createXMLData(
                "select 'scan_qualifiers=\"'"+
                "||SCAN_QUALIFIERS|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", SCAN_QUALIFIERS);

        createXMLData(
                "select 'next_qualifiers=\"'"+
                "||NEXT_QUALIFIERS|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", NEXT_QUALIFIERS);

        createXMLData(
                "select 'scanned_object=\"'"+
                "||SCAN_OBJECT_NAME|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", SCANNED_OBJECT);

        createXMLData(
                "select 'scan_type=\"'"+
                "||TRIM(SCAN_TYPE)|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", SCAN_TYPE);

        createXMLData(
                "select 'sort_type=\"'"+
                "||TRIM(SORT_TYPE)|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SORT_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", SORT_TYPE);

        createXMLData(
                "select 'sorter_output=\"'"+
                "||TRIM(CHAR(NO_OUTPUT_ROWS))|| '\"' " +
                "from ("+schema+".SYSXPLAIN_SORT_PROPS " +
                "NATURAL RIGHT OUTER JOIN "+schema+".SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = '"+getQuery()+"'", NO_OF_OUTPUT_ROWS_BY_SORTER);

    }

    /**
     * Generating the XML tree
     * @return all xml elements as a String
     */
    public String getXmlString(){

        for(int i=0;i<data.length;i++){
            //assume only one root element for any query
            if(data[i].getDepth()==0){//root element
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
    private void getChildren(int currentLevel,String id ){
        if(currentLevel <= depth){
            for(int i=0;i<data.length;i++){
                if(data[i].getDepth()== currentLevel &&
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
            str +="    ";

        return str;
    }

    /**
     * marking the depth of each element
     */
    public void markTheDepth(){
        int i=0;
        while(data[i].getParent().indexOf("null")== -1)
            i++;
        data[i].setDepth(depth); //root
        findChildren(i,depth);
    }

    /**
     *
     * @param idx current element's index
     * @param dep current examining depth
     */
    private void findChildren(int idx, int dep) {
        if(dep>depth)
            depth =dep;

        for(int i=0;i<data.length;i++){

            if(data[i].getParent().indexOf("null")== -1){
                if((data[idx].getId()).indexOf(data[i].getParent()) != -1
                        && i != idx)
                {
                    data[i].setDepth(dep +1);
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
                case ID:
                    data[i].setId(text+" ");
                    break;
                case P_ID:
                    data[i].setParent(text);
                    break;
                case NODE_TYPE:
                    data[i].setNodeType(text+" ");
                    break;
                case NO_OF_OPENS:
                    data[i].setNoOfOpens(text+" ");
                    break;
                case INPUT_ROWS:
                    data[i].setInputRows(text+" ");
                    break;
                case RETURNED_ROWS:
                    data[i].setReturnedRows(text+" ");
                    break;
                case VISITED_PAGES:
                    data[i].setVisitedPages(text+" ");
                    break;
                case SCAN_QUALIFIERS:
                    data[i].setScanQualifiers(text+" ");
                    break;
                case NEXT_QUALIFIERS:
                    data[i].setNextQualifiers(text+" ");
                    break;
                case SCANNED_OBJECT:
                    data[i].setScannedObject(text+" ");
                    break;
                case SCAN_TYPE:
                    data[i].setScanType(text+" ");
                    break;
                case SORT_TYPE:
                    data[i].setSortType(text+" ");
                    break;
                case NO_OF_OUTPUT_ROWS_BY_SORTER:
                    data[i].setSorterOutput(text+" ");
                    break;
                }
            }
            else{
                /*Other attributes are omitted from the xml document
                 * if they're null.
                 * P_ID can be null at the root.
                 * */
                switch(x){
                case P_ID:
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
    private int noOfNodes() throws SQLException{

        stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery(
                "select count(*) from " +
                ""+schema+".SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = '"+getQuery()+"'");
        results.next();
        int no = results.getInt(1);
        results.close();
        stmt.close();
        return no;
    }


    /**
     *
     * @return the &lt;statement&gt; element
     * @throws SQLException
     */
    public String statement() throws SQLException{
        stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery(
                "select STMT_TEXT "+
                "from "+schema+".SYSXPLAIN_STATEMENTS " +
                "where STMT_ID = '"+getQuery()+"'");
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
    private String replace(String stmt, String expr, String replace){
        String[] part=stmt.split(expr);
        String newStmt= part[0];
        for(int i=1;i<part.length;i++){
            newStmt += " "+replace+" "+part[i];
        }

        return newStmt;
    }

    /**
     *
     * @return XPLAIN_TIME of SYSXPLAIN_STATEMENTS
     * @throws SQLException
     */
    public String time() throws SQLException{
        stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery(
                "select '<time>'||TRIM(CHAR(XPLAIN_TIME))||" +
                "'</time>' from "+schema+".SYSXPLAIN_STATEMENTS " +
                "where STMT_ID = '"+getQuery()+"'");
        results.next();
        String time = results.getString(1);
        results.close();
        stmt.close();

        return time+"\n";
    }

    /**
     *
     * @return stmt_id as a XML element
     */
    public String stmtID(){
        return "<stmt_id>"+getQuery()+"</stmt_id>\n";
    }

    /**
     * closing the connection to the database
     */
    public void closeConnection()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }
            if (conn != null)
            {
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

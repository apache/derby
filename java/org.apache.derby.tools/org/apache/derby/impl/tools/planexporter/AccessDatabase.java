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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.iapi.tools.ToolUtils;

/**
 * This class will perform the database connection establishment,
 * querying the database, shut downing the database.
 * Created under DERBY-4587-PlanExporter tool
 */
public class AccessDatabase {

    private final Connection conn;
    private final String schema;
    private final String query;
    private final boolean schemaExists;

    private TreeNode[] data;

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
    public AccessDatabase(String dburl, String aSchema, String aQuery)
            throws InstantiationException, IllegalAccessException,
                   ClassNotFoundException, SQLException, NoSuchMethodException, InvocationTargetException
    {
        this(createConnection(dburl), aSchema, aQuery);
    }

    /**
     *
     * @param aConn
     * @param aSchema
     * @param aQuery
     *
     */
    public AccessDatabase(Connection aConn, String aSchema, String aQuery)
            throws SQLException
    {

        conn = aConn;
        schema = aSchema;
        query = aQuery;
        schemaExists = schemaExists();

        if (schemaExists) {
            setSchema();
        }
    }

    /**
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static Connection createConnection(String dbURL)
            throws InstantiationException, IllegalAccessException,
                   ClassNotFoundException, SQLException, NoSuchMethodException,
                   InvocationTargetException
    {

        Class<?> clazz = (dbURL.indexOf("://") != -1) ?
          Class.forName("org.apache.derby.jdbc.ClientDriver")
          :
          Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        clazz.getConstructor().newInstance();

        //Get a connection
        return DriverManager.getConnection(dbURL);

    }

    /**
     * Set the schema of the current connection to the XPLAIN schema in
     * which the statistics can be found.
     *
     * @throws SQLException if an error happens while accessing the database
     */
    private void setSchema() throws SQLException {
        PreparedStatement setSchema = conn.prepareStatement("SET SCHEMA ?");
        setSchema.setString(1, schema);
        setSchema.execute();
        setSchema.close();
    }

    /**
     * Check if there is a schema in the database that matches the schema
     * name that was passed in to this instance.
     */
    private boolean schemaExists() throws SQLException {
    	ResultSet result = conn.getMetaData().getSchemas();
        try {
            while (result.next()) {
                if (result.getString(1).equals(schema)) {
                    // Found it!
                    return true;
                }
            }
        } finally {
            result.close();
        }

        // Didn't find the schema.
        return false;
    }

    public boolean verifySchemaExistance() {
        return schemaExists;
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
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", ID);

        createXMLData(
                "select PARENT_RS_ID "+
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", P_ID);

        createXMLData(
                "select 'name=\"' ||OP_IDENTIFIER|| '\"' " +
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", NODE_TYPE);

        createXMLData(
                "select 'no_opens=\"' " +
                "|| TRIM(CHAR(NO_OPENS))|| '\"' " +
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", NO_OF_OPENS);

        createXMLData(
                "select 'input_rows=\"' " +
                "|| TRIM(CHAR(INPUT_ROWS))|| '\"' " +
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", INPUT_ROWS);

        createXMLData(
                "select 'returned_rows=\"' " +
                "|| TRIM(CHAR(RETURNED_ROWS))|| '\"' " +
                "from SYSXPLAIN_RESULTSETS " +
                "where STMT_ID = ?", RETURNED_ROWS);

        createXMLData(
                "select 'visited_pages=\"'" +
                "|| TRIM(CHAR(NO_VISITED_PAGES))|| '\"' " +
                "from (SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", VISITED_PAGES);

        createXMLData(
                "select 'scan_qualifiers=\"'"+
                "||SCAN_QUALIFIERS|| '\"' " +
                "from (SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", SCAN_QUALIFIERS);

        createXMLData(
                "select 'next_qualifiers=\"'"+
                "||NEXT_QUALIFIERS|| '\"' " +
                "from (SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", NEXT_QUALIFIERS);

        createXMLData(
                "select 'scanned_object=\"'"+
                "||SCAN_OBJECT_NAME|| '\"' " +
                "from (SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", SCANNED_OBJECT);

        createXMLData(
                "select 'scan_type=\"'"+
                "||TRIM(SCAN_TYPE)|| '\"' " +
                "from (SYSXPLAIN_SCAN_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", SCAN_TYPE);

        createXMLData(
                "select 'sort_type=\"'"+
                "||TRIM(SORT_TYPE)|| '\"' " +
                "from (SYSXPLAIN_SORT_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", SORT_TYPE);

        createXMLData(
                "select 'sorter_output=\"'"+
                "||TRIM(CHAR(NO_OUTPUT_ROWS))|| '\"' " +
                "from (SYSXPLAIN_SORT_PROPS " +
                "NATURAL RIGHT OUTER JOIN SYSXPLAIN_RESULTSETS) " +
                "where STMT_ID = ?", NO_OF_OUTPUT_ROWS_BY_SORTER);

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
     * Create XML data based on the query that's passed in. The query should
     * have exactly one parameter, which will be initialized to the statement
     * id before the query is executed.
     *
     * @param qry query to be executed
     * @throws SQLException
     */
    private void createXMLData(String qry, int x) throws SQLException{

        PreparedStatement ps = conn.prepareStatement(qry);
        ps.setString(1, getQuery());

        ResultSet results = ps.executeQuery();

        int i=0;
        while(results.next())
        {
            String text= results.getString(1);

            if(text != null){

                /*Removing possible occurrences of special XML characters
                 * from XML node attributes in XML representation.*/
                text = escapeInAttribute(text);

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
        ps.close();
    }

    /**
     *
     * @return total # of nodes
     * @throws SQLException
     */
    private int noOfNodes() throws SQLException{
        PreparedStatement ps = conn.prepareStatement(
                "select count(*) from SYSXPLAIN_RESULTSETS where STMT_ID = ?");
        ps.setString(1, getQuery());
        ResultSet results = ps.executeQuery();
        results.next();
        int no = results.getInt(1);
        results.close();
        ps.close();
        return no;
    }


    /**
     *
     * @return the &lt;statement&gt; element
     * @throws SQLException
     */
    public String statement() throws SQLException{
        PreparedStatement ps = conn.prepareStatement(
                "select STMT_TEXT from SYSXPLAIN_STATEMENTS where STMT_ID = ?");
        ps.setString(1, getQuery());
        ResultSet results = ps.executeQuery();
        results.next();
        String statement = results.getString(1);
        results.close();
        ps.close();

        /*Removing possible occurrences of special XML characters
         * from a query statement with XML representation.*/
        statement = escapeForXML(statement);

        return "<statement>"+statement+"</statement>\n";
    }

    /**
     * Escape characters that have a special meaning in XML.
     *
     * @param text the text to escape
     * @return the text with special characters escaped
     */
    private static String escapeForXML(String text) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '&':
                    sb.append("&amp;");
                    break;
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                case '\'':
                    sb.append("&apos;");
                    break;
                case '"':
                    sb.append("&quot;");
                    break;
                default:
                    sb.append(ch);
            }
        }

        return sb.toString();
    }

    /**
     * This method is needed since in the case of XML attributes
     * we have to filter the quotation (&quot;) marks that is compulsory.
     * eg:
     * scanned_object="A &quot;quoted&quot;  table name";
     *
     * @param text attribute string to be checked
     * @return modified string
     */
    private String escapeInAttribute(String text) {
        if (text.indexOf('"') == -1)
            return text;
        String correctXMLString = escapeForXML(
                text.substring(text.indexOf('"') + 1, text.length() - 1));
        return text.substring(0,text.indexOf('"')+1)+correctXMLString+"\"";
    }
   
    /**
     *
     * @return XPLAIN_TIME of SYSXPLAIN_STATEMENTS
     * @throws SQLException
     */
    public String time() throws SQLException{
        PreparedStatement ps = conn.prepareStatement(
                "select '<time>'||TRIM(CHAR(XPLAIN_TIME))||" +
                "'</time>' from SYSXPLAIN_STATEMENTS " +
                "where STMT_ID = ?");
        ps.setString(1, getQuery());
        ResultSet results = ps.executeQuery();
        results.next();
        String time = results.getString(1);
        results.close();
        ps.close();

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
        return (TreeNode[]) ToolUtils.copy( data );
    }
}

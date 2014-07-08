/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.IOException;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import javax.xml.parsers.DocumentBuilderFactory;
import junit.framework.Test;
import org.apache.derby.optional.api.LuceneIndexDescriptor;
import org.apache.derby.optional.api.LuceneUtils;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * <p>
 * Basic test of the optional tool which provides Lucene indexing of
 * columns in Derby tables.
 * </p>
 */
public class LuceneSupportTest extends BaseJDBCTestCase {

    private static  final   String  ILLEGAL_CHARACTER = "42XBD";
    
	public LuceneSupportTest(String name) {
		super(name);
	}
	
	public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("LuceneSupportTest");

        Test    baseTest = TestConfiguration.embeddedSuite(LuceneSupportTest.class);
        Test        singleUseTest = TestConfiguration.singleUseDatabaseDecorator( baseTest );
        Test        localizedTest = new LocaleTestSetup( singleUseTest, new Locale( "en", "US" ) );
		
		suite.addTest(SecurityManagerSetup.noSecurityManager(localizedTest));
 
		return suite;
	}
	
	public void testCreateAndQueryIndex() throws Exception {
		CallableStatement cSt;
		Statement s = createStatement();

        // verify that we are in an en Locale
        getConnection().prepareStatement
            (
             "create function getDatabaseLocale() returns varchar( 20 )\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.LuceneSupportPermsTest.getDatabaseLocale()'\n"
             ).executeUpdate();
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "values ( substr( getDatabaseLocale(), 1, 2 ) )"
              ),
             new String[][]
             {
                 { "en" }
             }
             );
        getConnection().prepareStatement( "drop function getDatabaseLocale" ).executeUpdate();
	    
		cSt = prepareCall
            ( "call LuceneSupport.createIndex('lucenetest','titles','title', null )" );
	    assertUpdateCount(cSt, 0);
	    
	    String[][] expectedRows = new String[][]
            {
                { "1","0","0.8048013" },
	    		{ "3","2","0.643841" }
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select * from table ( lucenetest.titles__title( 'grapes', 1000, null ) ) luceneResults"
              ),
             expectedRows
             );

	    expectedRows = new String[][]
            {
	    		{ "3","2","0.643841" }
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select * from table ( lucenetest.titles__title( 'grapes', 1000, .75 ) ) luceneResults"
              ),
             expectedRows
             );

	    JDBC.assertEmpty
            (
             s.executeQuery
             (
              "select * from table ( lucenetest.titles__title( 'grapes',  1000, 0.5) ) luceneResults"
              )
             );

	    expectedRows = new String[][]
            {
                { "The Grapes Of Wrath", "John Steinbeck", "The Viking Press", "0"},
	    		{"Vines, Grapes, and Wines", "Jancis Robinson", "Alfred A. Knopf", "2"}
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select title, author, publisher, documentID\n" +
              "from lucenetest.titles t, table ( lucenetest.titles__title( 'grapes', 1000, null ) ) l\n" +
              "where t.id = l.id\n" 
              ),
             expectedRows
             );
	   
		cSt = prepareCall
            ( "call LuceneSupport.dropIndex('lucenetest','titles','title')" );
	    assertUpdateCount(cSt, 0);

	}
	
	public void testUpdateIndex() throws Exception {
		CallableStatement cSt;
		Statement s = createStatement();
		
		cSt = prepareCall
            ( "call LuceneSupport.createIndex('lucenetest','titles','title', null)" );
	    assertUpdateCount(cSt, 0);

	    JDBC.assertEmpty
            (
             s.executeQuery
             (
              "select *\n" +
              "from table ( lucenetest.titles__title( 'mice', 1000, null ) ) luceneResults\n"
              )
             );
	    
	    cSt = prepareCall( "update TITLES SET TITLE='Of Mice and Men' WHERE ID=1" );
	    assertUpdateCount(cSt, 1);
	    
	    JDBC.assertEmpty
            (
             s.executeQuery
             (
              "select *\n" +
              "from table ( lucenetest.titles__title( 'mice', 1000, null ) ) luceneResults\n"
              )
             );
	    
		cSt = prepareCall
            ( "call LuceneSupport.updateIndex('lucenetest','titles','title', null)" );
	    assertUpdateCount(cSt, 0);

	    String[][] expectedRows = new String[][]
            {
                { "1","0","1.058217" }
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select *\n" +
              "from table ( lucenetest.titles__title( 'mice', 1000, null ) ) luceneResults\n"
              ),
             expectedRows
             );

		cSt = prepareCall
            ( "call LuceneSupport.dropIndex('lucenetest','titles','title')" );
	    assertUpdateCount(cSt, 0);

	}
	
	public void testListIndex() throws Exception {
		CallableStatement cSt;
		Statement s = createStatement();

	    cSt = prepareCall
            ( "call LuceneSupport.createIndex('lucenetest','titles','title', null)" );
	    assertUpdateCount(cSt, 0);
	    
		cSt = prepareCall
            ( "call LuceneSupport.createIndex('lucenetest','titles','author', null)" );
	    assertUpdateCount(cSt, 0);
	    
	    // leave out lastmodified as the date will change
	    String[][] expectedRows = new String[][]
            {
                { "LUCENETEST", "TITLES", "AUTHOR" },
	    		{ "LUCENETEST", "TITLES", "TITLE" }
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select schemaname, tablename, columnname from table ( LuceneSupport.listIndexes() ) listindexes order by schemaname, tablename, columnname"
              ),
             expectedRows
             );

		cSt = prepareCall
            ( "call LuceneSupport.dropIndex('lucenetest','titles','title')" );
	    assertUpdateCount(cSt, 0);

	    expectedRows = new String[][]
            {
                { "LUCENETEST", "TITLES", "AUTHOR" },
            };
	    JDBC.assertFullResultSet
            (
             s.executeQuery
             (
              "select schemaname, tablename, columnname from table ( LuceneSupport.listIndexes() ) listindexes order by schemaname, tablename, columnname"
              ),
             expectedRows
             );

		cSt = prepareCall
            ( "call LuceneSupport.dropIndex('lucenetest','titles','author')" );
	    assertUpdateCount(cSt, 0);
	    
	    JDBC.assertEmpty
            (
             s.executeQuery
             (
              "select schemaname, tablename, columnname from table ( LuceneSupport.listIndexes() ) listindexes"
              )
             );

	}
	
	public void testDropIndexBadCharacters() throws Exception {
		CallableStatement st;
	    
		assertCallError( ILLEGAL_CHARACTER, "call LuceneSupport.dropIndex('../','','')");
		assertCallError( ILLEGAL_CHARACTER, "call LuceneSupport.dropIndex('','../','')");
		assertCallError( ILLEGAL_CHARACTER, "call LuceneSupport.dropIndex('','','../')");
		
	}

    //////////////////////////////////////////////////////////////
    //
    //  BEGIN TEST FOR MULTIPLE FIELDS
    //
    //////////////////////////////////////////////////////////////
	
    public void testMultipleFields() throws SQLException
    {
        println( "Running multi-field test." );
        
        Statement s = createStatement();

        s.execute("create table multifield(id int primary key, c clob)");
        s.execute("insert into multifield values "
                + "(1, '<document><secret/>No one must know!</document>'), "
                + "(2, '<document>No secret here!</document>')");

        s.execute("call lucenesupport.createindex('lucenetest', 'multifield', "
                  + "'c', '" + getClass().getName() + ".makeMultiFieldIndexDescriptor')");

        PreparedStatement ps = prepareStatement(
                "select id from table(multifield__c(?, 100, null)) t");

        String[][] bothRows = { {"1"}, {"2"} };

        ps.setString(1, "text:secret");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "2");
        ps.setString(1, "tags:secret");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        ps.setString(1, "secret");
        JDBC.assertUnorderedResultSet(ps.executeQuery(), bothRows);
    }

    /** Create the custom index descriptor for the multi-field test */
    public  static  LuceneIndexDescriptor   makeMultiFieldIndexDescriptor()
    {
        return new MultiFieldIndexDescriptor();
    }
    /**
     * Create a simple query parser for multiple fields, which uses
     * StandardAnalyzer instead of the XMLAnalyzer that was used to create
     * the index.
     */
    public static QueryParser createXMLQueryParser(
            Version version, String[] fields, Analyzer analyzer) {
        return new MultiFieldQueryParser(
                version, fields, new StandardAnalyzer(version));
    }

    /**
     * Custom analyzer for XML files. It indexes the tags and the text
     * separately.
     */
    public static class XMLAnalyzer extends Analyzer {

        public XMLAnalyzer() {
            // We want different tokenizers for different fields. Set reuse
            // policy to per-field to achieve that.
            super(PER_FIELD_REUSE_STRATEGY);
        }

        @Override
        protected TokenStreamComponents createComponents(
                String fieldName, Reader reader) {

            if (fieldName.equals("text")) {
                return new TokenStreamComponents(new XMLTextTokenizer(reader));
            }

            if (fieldName.equals("tags")) {
                return new TokenStreamComponents(new XMLTagsTokenizer(reader));
            }

            fail("unknown field name: " + fieldName);
            return null;
        }
    }

    /** Common logic for XMLTextTokenizer and XMLTagsTokenizer. */
    private abstract static class AbstractTokenizer extends Tokenizer {
        Iterator<String> tokens;
        final CharTermAttribute charTermAttr =
                addAttribute(CharTermAttribute.class);
        final PositionIncrementAttribute posIncrAttr
                = addAttribute(PositionIncrementAttribute.class);

        AbstractTokenizer(Reader reader) {
            super(reader);
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (tokens == null) {
                tokens = getTokens().iterator();
            }

            if (tokens.hasNext()) {
                charTermAttr.setEmpty();
                charTermAttr.append(tokens.next());
                posIncrAttr.setPositionIncrement(1);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void reset() throws IOException {
            tokens = null;
            super.reset();
        }

        abstract Iterable<String> getTokens();
    }

    private static class XMLTextTokenizer extends AbstractTokenizer {

        XMLTextTokenizer(Reader in) {
            super(in);
        }

        @Override
        Iterable<String> getTokens() {
            StringBuilder text = new StringBuilder();
            getAllText(parseXMLDocument(input), text);
            return Arrays.asList(text.toString().split("[ \r\n\t]"));
        }

    }

    private static class XMLTagsTokenizer extends AbstractTokenizer {

        XMLTagsTokenizer(Reader in) {
            super(in);
        }

        @Override
        Iterable<String> getTokens() {
            return getAllXMLTags(parseXMLDocument(input));
        }

    }

    /** Parse an XML document from a Reader. */
    private static Document parseXMLDocument(Reader reader) {
        Document doc = null;

        try {
            doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder().parse(new InputSource(reader));
            reader.close();
        } catch (Exception e) {
            fail("Failed to parse XML document", e);
        }

        return doc;
    }

    /** Get a list of all the XML tags in a node. */
    private static List<String> getAllXMLTags(Node node) {
        ArrayList<String> list = new ArrayList<String>();
        NodeList nl = node.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                list.add(n.getNodeName());
                list.addAll(getAllXMLTags(n));
            }
        }
        return list;
    }

    /** Strip out all tags from an XML node, so that only the text is left. */
    private static void getAllText(Node node, StringBuilder sb) {
        if (node.getNodeType() == Node.TEXT_NODE) {
            sb.append(node.getNodeValue());
        } else {
            NodeList nl = node.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                getAllText(nl.item(i), sb);
            }
        }
    }

    public static class MultiFieldIndexDescriptor implements LuceneIndexDescriptor
    {
        public  String[]    getFieldNames() { return new String[] { "tags", "text" }; }
    
        public Analyzer getAnalyzer()   { return new XMLAnalyzer(); }

        public  QueryParser getQueryParser()
        {
            Version version = LuceneUtils.currentVersion();
            
            return new MultiFieldQueryParser
                (
                 version,
                 getFieldNames(),
                 new StandardAnalyzer( version )
                 );
        }

    }

    //////////////////////////////////////////////////////////////
    //
    //  END TEST FOR MULTIPLE FIELDS
    //
    //////////////////////////////////////////////////////////////
	
	protected void setUp() throws SQLException {
		CallableStatement cSt;			    
		Statement st = createStatement();
		
		try {
			st.executeUpdate("create schema lucenetest");
		} catch (Exception e) {
		}
		st.executeUpdate("set schema lucenetest");	
		st.executeUpdate("create table titles (ID int generated always as identity primary key, ISBN varchar(16), PRINTISBN varchar(16), title varchar(1024), subtitle varchar(1024), author varchar(1024), series varchar(1024), publisher varchar(1024), collections varchar(128), collections2 varchar(128))");
		st.executeUpdate("insert into titles (ISBN, PRINTISBN, TITLE, SUBTITLE, AUTHOR, SERIES, PUBLISHER, COLLECTIONS, COLLECTIONS2) values ('9765087650324','9765087650324','The Grapes Of Wrath','The Great Depression in Oklahoma','John Steinbeck','Noble Winners','The Viking Press','National Book Award','Pulitzer Prize')");
		st.executeUpdate("insert into titles (ISBN, PRINTISBN, TITLE, SUBTITLE, AUTHOR, SERIES, PUBLISHER, COLLECTIONS, COLLECTIONS2) values ('6754278542987','6754278542987','Identical: Portraits of Twins','Best Photo Book 2012 by American Photo Magazine','Martin Schoeller','Portraits','teNeues','Photography','')");
		st.executeUpdate("insert into titles (ISBN, PRINTISBN, TITLE, SUBTITLE, AUTHOR, SERIES, PUBLISHER, COLLECTIONS, COLLECTIONS2) values ('2747583475882','2747583475882','Vines, Grapes, and Wines','The wine drinker''s guide to grape varieties','Jancis Robinson','Reference','Alfred A. Knopf','Wine','')");	
		st.executeUpdate("insert into titles (ISBN, PRINTISBN, TITLE, SUBTITLE, AUTHOR, SERIES, PUBLISHER, COLLECTIONS, COLLECTIONS2) values ('4356123483483','4356123483483','A Tale of Two Cities','A fictional account of events leading up to the French revolution','Charles Dickens','Classics','Chapman & Hall','Fiction','Social Criticism')");	

		cSt = prepareCall
            ( "call syscs_util.syscs_register_tool('luceneSupport',true)" );
	    assertUpdateCount(cSt, 0);

	}
	
	protected void tearDown() throws Exception {
		CallableStatement cSt;
		Statement st = createStatement();
		
		st.executeUpdate("drop table titles");
		
		cSt = prepareCall
            ( "call syscs_util.syscs_register_tool('luceneSupport',false)" );
	    assertUpdateCount(cSt, 0);
	    super.tearDown();
	}
}

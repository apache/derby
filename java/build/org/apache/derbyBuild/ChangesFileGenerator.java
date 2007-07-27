/*  Derby - Class org.apache.derbyBuild.ReleaseNotesGenerator

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

package org.apache.derbyBuild;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.taskdefs.Echo;

/**
 * <p>
 * This tool generates the Changes file for a Derby release. See the USAGE
 * constant for details on how to run this tool standalone. It is recommended
 * that you freshly regenerate your BUG_LIST just before you run this tool.
 * </p>
 *
 * <p>
 * The tool is designed to be run from Derby's ant build scripts. To run under
 * ant, do the following:
 * </p>
 *
 * <ul>
 * <li>Define the "relnotes.src.reports" variable in your ant.properties. This
 * variable points at the directory which holds your xml JIRA reports.</li>
 * <li>Put your xml JIRA reports in that directory. They should have the
 * following names:
 *  <ul>
 *  <li>fixedBugsList.xml - This is the list of issues fixed by the release.</li>
 *  </ul>
 * </li>
 * <li>You can use java org.apache.derbyBuild.JiraConnector to create this file.
 * </li>
 * <li>Then cd to tools/release and run ant thusly: "ant genchanges"</li>
 * </ul>
 *
 * <p>For more information on this tool, please see the JIRA which introduce its companion, ReleaseNotesGenerator:
 * </p>
 *
 * <p>
 * <a href="http://issues.apache.org/jira/browse/DERBY-2570">DERBY-2570</a>
 * </p>
 */
public class ChangesFileGenerator extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////
    
    private static  final   String  USAGE =
        "Usage:\n" +
        "\n" +
        "  java org.apache.derbyBuild.ChangesFileGenerator SUMMARY BUG_LIST OUTPUT_PAMPHLET\n" +
        "\n" +
        "    where\n" +
        "                  BUG_LIST                     An xml JIRA report of issues addressed by this release.\n" +
        "                  OUTPUT_PAMPHLET  The output file to generate, typically CHANGES.html.\n" +
        "\n" +
        "The ChangesFileGenerator attempts to connect to issues.apache.org in\n" +
        "order to find the individual JIRAs. Before running this program, make sure that you can\n" +
        "ping issues.apache.org.\n" +
        "\n" +
        "The ChangesFileGenerator assumes that the two JIRA reports contain\n" +
        "key, title, and attachments elements for each Derby issue.\n" +
        "For this reason, it is recommended that you freshly generate BUG_LIST\n" +
        "just before you run this tool.\n"
        ;

    // header levels
    private static  final   int     BANNER_LEVEL = 1;
    private static  final   int     MAIN_SECTION_LEVEL = BANNER_LEVEL + 1;
    private static  final   int     ISSUE_DETAIL_LEVEL = MAIN_SECTION_LEVEL + 1;
    
    // major sections
    private static  final   String  BUG_FIXES_SECTION = "CHANGES";

    // headlines
    private static  final   String  BRANCH_HEADLINE = "Branch";
    private static  final   String  DESCRIPTION_HEADLINE = "Description";
    private static  final   String  ISSUE_ID_HEADLINE = "Issue Id";

    // formatting tags
    private static  final   String  ANCHOR = "a";
    private static  final   String  BODY = "body";
    private static  final   String  BOLD = "b";
    private static  final   String  BORDER = "border";
    private static  final   String  COLUMN = "td";
    private static  final   String  HORIZONTAL_LINE = "hr";
    private static  final   String  HTML = "html";
    private static  final   String  INDENT = "blockquote";
    private static  final   String  LIST = "ul";
    private static  final   String  LIST_ELEMENT = "li";
    private static  final   String  NAME = "name";
    private static  final   String  PARAGRAPH = "p";
    private static  final   String  ROW = "tr";
    private static  final   String  TABLE = "table";

    // tags in summary xml
    private static  final   String  SUM_BRANCH = "branch";
    private static  final   String  SUM_OVERVIEW = "overview";
    private static  final   String  SUM_PREVIOUS_RELEASE_ID = "previousReleaseID";
    private static  final   String  SUM_RELEASE_ID = "releaseID";

    // tags in JIRA reports
    private static  final   String  JIRA_ID = "id";
    private static  final   String  JIRA_ITEM = "item";
    private static  final   String  JIRA_KEY = "key";
    private static  final   String  JIRA_NAME = "name";
    private static  final   String  JIRA_TITLE = "title";
    private static  final   String  JIRA_FIXIN = "fixVersion";

    // managing releaseNote.html
    private static  final   String  RN_SUMMARY_OF_CHANGE = "Summary of Change";

    // other JIRA control
    private static  final   String  RN_H4 = "h4";
    
    // other html control
    private static  final   int     DEFAULT_TABLE_BORDER_WIDTH = 2;
    private static  final   String  XML_DECLARATION = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    // set on the command line or by ant
    private String  _summaryFileName;
    private String  _bugListFileName;
    private String  _pamphletFileName;

    // computed at run time
    private String  _releaseID;
    private String  _previousReleaseID;

    // misc
    private boolean _invokedByAnt = true;
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  INNER CLASSES
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * State used by the generator
     * </p>
     */
    public  static  class   GeneratorState
    {
        private DocumentBuilder _documentBuilder;
        private Document    _pamphlet;
        private Document    _summary;
        private Document    _bugList;
        private ArrayList       _errors;

        public  GeneratorState
            (
             DocumentBuilder    documentBuilder,
             Document   pamphlet,
             Document   summary,
             Document   bugList
             )
        {
            _documentBuilder = documentBuilder;
            _pamphlet = pamphlet;
            _summary = summary;
            _bugList = bugList;

            _errors = new ArrayList();
        }

        public  void    addError( String message )
        {
            _errors.add( message );
        }

        public  DocumentBuilder getDocumentBuilder() { return _documentBuilder; }
        public  Document    getPamphlet() { return _pamphlet; }
        public  Document    getSummary() { return _summary; }
        public  Document    getBugList() { return _bugList; }
        
        public  String[]     getErrors()
        {
            String[]        squeezed = new String[ _errors.size() ];

            _errors.toArray( squeezed );

            return squeezed;
        }
    }

    /**
     * <p>
     * An issue from a JIRA report.
     * </p>
     */
    public  static  class   JiraIssue
    {
        private String  _key;
        private String  _title;

        public  JiraIssue
            (
             String key,
             String title
             )
        {
            _key = key;
            _title = title;
        }

        public  String  getKey() { return _key; }
        public  String  getTitle() { return _title; }
        
        public  String  getJiraAddress()
        {
            return "http://issues.apache.org/jira/browse/" + _key;
        }

    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    //
    //  ENTRY POINT
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Generate the changes file (for details on how to invoke this tool, see
     * the header comment on this class).
     * </p>
     */
    public  static  void    main( String[] args )
        throws Exception
    {
        ChangesFileGenerator   me = new ChangesFileGenerator();

        me._invokedByAnt = false;
        
        if ( me.parseArgs( args ) ) { me.execute(); }
        else { me.printUsage(); }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  ANT Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    /** Ant accessor to set the name of the summary file prepared by the Release Manager */
    public   void    setSummaryFileName( String summaryFileName ) { _summaryFileName = summaryFileName; }     

    /** Ant accessor to set the name of the JIRA-generated list of bugs addressed by this release */
    public   void    setBugListFileName( String bugListFileName ) { _bugListFileName = bugListFileName; }     

    /** Ant accessor to set the name of the generated output file */
    public   void    setPamphletFileName( String pamphletFileName ) { _pamphletFileName = pamphletFileName; }     
        
    /**
     * <p>
     * This is Ant's entry point into this task.
     * </p>
     */
    public  void    execute()
        throws BuildException
    {
        try {
            GeneratorState                  gs = initialize();

            beginPamphlet( gs );
            buildBugList( gs );
            replaceVariables( gs );
            endPamphlet( gs );
            
            printPamphlet( gs );
            printErrors( gs );
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            
            throw new BuildException( "Error running ChangeFileGenerator: " + t.getMessage(), t );
        }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  OTHER ACCESSORS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the release ID from the summary file.
     * </p>
     */
    private String getReleaseID( GeneratorState gs)
        throws Exception
    {
        if ( _releaseID == null )
        {
            Document    summary = gs.getSummary();
            Element     summaryRoot = summary.getDocumentElement();
            
            _releaseID = squeezeText( getFirstChild( summaryRoot, SUM_RELEASE_ID ) );
        }
        
        return _releaseID;
    }
    
    /**
     * <p>
     * Get the previous release ID from the summary file.
     * </p>
     */
    private String getPreviousReleaseID( GeneratorState gs)
        throws Exception
    {
        if ( _previousReleaseID == null )
        {
            Document    summary = gs.getSummary();
            Element     summaryRoot = summary.getDocumentElement();
            
            _previousReleaseID = squeezeText( getFirstChild( summaryRoot, SUM_PREVIOUS_RELEASE_ID ) );
        }
        
        return _previousReleaseID;
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    //////////////////////////////////
    //
    //  Boilerplate
    //
    //////////////////////////////////

    /**
     * <p>
     * Start an html docment. Returns the body element.
     * </p>
     */
    private void beginPamphlet( GeneratorState gs )
        throws Exception
    {
        Document    pamphlet = gs.getPamphlet();
        String          releaseID = getReleaseID( gs );
        String          titleText = "Changes for Derby " + releaseID;
        Element     html = pamphlet.createElement( HTML );
        Element     title = createTextElement( pamphlet, "title", titleText );
        Element     body = pamphlet.createElement( BODY );

        pamphlet.appendChild( html );
        html.appendChild( title );
        html.appendChild( body );
        
        Element     bannerBlock = createHeader( body, BANNER_LEVEL, titleText );
        buildDelta( gs, bannerBlock );
        
        Element     toc = createList( body );

        createSection( body, MAIN_SECTION_LEVEL, toc, BUG_FIXES_SECTION, BUG_FIXES_SECTION );
    }
    
    /**
     * <p>
     * Finish the html document.
     * </p>
     */
    private void    endPamphlet( GeneratorState gs )
        throws Exception
    {
    }
    
    //////////////////////////////////
    //
    //  Delta SECTION
    //
    //////////////////////////////////

    /**
     * <p>
     * Note that this release is a delta from the previous one.
     * </p>
     */
    private void buildDelta( GeneratorState gs, Element parent )
        throws Exception
    {
        String          releaseID = getReleaseID( gs );
        String          previousReleaseID = getPreviousReleaseID( gs );
        String          deltaStatement =
            "These notes describe the difference between Derby release " + releaseID +
            " and the preceding release " + previousReleaseID + ".";

//        addParagraph( parent, deltaStatement );
    }
    
    //////////////////////////////////
    //
    //  Bug List SECTION
    //
    //////////////////////////////////

    /**
     * <p>
     * Build the Bug List section.
     * </p>
     */
    private void buildBugList( GeneratorState gs )
        throws Exception
    {
        Document    pamphlet = gs.getPamphlet();
        Element     bugListSection = getSection( pamphlet, MAIN_SECTION_LEVEL, BUG_FIXES_SECTION );
        Document    bugList = gs.getBugList();
        JiraIssue[]    bugs = getJiraIssues( bugList );
        int                 count = bugs.length;
        String          releaseID = getReleaseID( gs );
        String          previousReleaseID = getPreviousReleaseID( gs );
        String          deltaStatement =
            "The following table lists issues in JIRA which were fixed between " +
            "Derby release " + releaseID + " and the preceding release " + 
            previousReleaseID + ". This includes issues for the product " +
            "source, documentation and tests";

        addParagraph( bugListSection, deltaStatement );

        Element     table = createTable
            ( bugListSection, DEFAULT_TABLE_BORDER_WIDTH, new String[] { ISSUE_ID_HEADLINE, DESCRIPTION_HEADLINE } );

        for ( int i = 0; i < count; i++ )
        {
        	if (bugs[i] == null)
        		continue;
            JiraIssue   issue = bugs[ i ];
            Element     row = insertRow( table );
            Element     linkColumn = insertColumn( row );
            Element     descriptionColumn = insertColumn( row );
            Element     hotlink = createLink( pamphlet, issue.getJiraAddress(), issue.getKey() );
            Text            title = pamphlet.createTextNode( issue.getTitle() );

            linkColumn.appendChild( hotlink );
            descriptionColumn.appendChild( title );
        }
    }
    
    //////////////////////////////////
    //
    //  REPLACE VARIABLES
    //
    //////////////////////////////////

    /**
     * <p>
     * Replace the known parameters with their corresponding text values.
     * </p>
     */
    private void replaceVariables( GeneratorState gs )
        throws Exception
    {
        Document    pamphlet = gs.getPamphlet();

        replaceTag( pamphlet, SUM_RELEASE_ID, getReleaseID( gs ) );
        replaceTag( pamphlet, SUM_PREVIOUS_RELEASE_ID, getPreviousReleaseID( gs ) );
    }
    
   //////////////////////////////////
    //
    //  Print the generated document.
    //
    //////////////////////////////////

    /**
     * <p>
     * Print pamphlet to output file.
     * </p>
     */
    private void    printPamphlet( GeneratorState gs )
        throws Exception
    {
        Document    pamphlet = gs.getPamphlet();
        Source            source = new DOMSource( pamphlet );
        File                  outputFile = new File( _pamphletFileName );
        Result            result = new StreamResult( outputFile );
        Transformer   transformer = TransformerFactory.newInstance().newTransformer();
        
        transformer.transform( source, result );        
    }
    
    //////////////////////////////////
    //
    //  Print errors
    //
    //////////////////////////////////

    /**
     * <p>
     * Print errors.
     * </p>
     */
    private void printErrors( GeneratorState gs )
        throws Exception
    {
        String[]     errors = gs.getErrors();
        int                 count = errors.length;

        if ( count > 0 )
        {
            println( "The following other errors occurred:" );

            for ( int i = 0; i < count; i++ )
            {
                String  error = errors[ i ];
                
                println( "\n" + error );
            }
        }
    }
    
    //////////////////////////////////
    //
    //  ARGUMENT MINIONS
    //
    //////////////////////////////////

    /**
     * <p>
     * Returns true if arguments parse successfully, false otherwise.
     * </p>
     */
    private boolean    parseArgs( String[] args )
        throws Exception
    {
        if ( (args == null) || (args.length != 4) ) { return false; }

        int     idx = 0;

        setSummaryFileName( args[ idx++ ] );
        setBugListFileName( args[ idx++ ] );
        setPamphletFileName( args[ idx++ ] );

        return true;
    }
    
    private void    printUsage()
    {
        println( USAGE );
    }

    /**
     * <p>
     * Make sure that the input files all exist. Returns a state variable for the generator.
     * </p>
     */
    private GeneratorState  initialize()
        throws Exception
    {
        DocumentBuilderFactory  factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder              builder = factory.newDocumentBuilder();
        Document                          pamphlet = builder.newDocument();
        File                                    summaryFile = new File( _summaryFileName );
        File                                    bugListFile = new File( _bugListFileName );
        Document                        summary = builder.parse( summaryFile );
        Document                        bugList = builder.parse( bugListFile );
        
        return new GeneratorState
            (
             builder,
             pamphlet,
             summary,
             bugList
             );
    }

    ////////////////////////////////////////////////////////
    //
    // HTML MINIONS
    //
    ////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a section at the end of a parent element and link to it from a
     * table of contents.
     * </p>
     */
    private Element createSection( Element parent, int sectionLevel, Element toc, String sectionName, String tocEntry )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Text                textNode = doc.createTextNode( tocEntry );

        return createSection( parent, sectionLevel, toc, sectionName, textNode );
    }
    
    /**
     * <p>
     * Create a section at the end of a parent element and link to it from a
     * table of contents.
     * </p>
     */
    private Element createSection( Element parent, int sectionLevel, Element toc, String sectionName, Node visibleText )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Element             link = createLocalLink( doc, sectionName, visibleText );

        addListItem( toc, link );

        return createHeader( parent, sectionLevel, sectionName );
    }
    
    /**
     * <p>
     * Create a header at the end of the parent node. Return the block created
     * to hold the text following this header.
     * </p>
     */
    private Element createHeader( Element parent, int headerLevel, String text )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Text            textNode = doc.createTextNode( text );
        Element         header = doc.createElement( makeHeaderTag( headerLevel ) );
        Element         anchor = doc.createElement( ANCHOR );
        Element         block =doc.createElement( INDENT );

        parent.appendChild( header );
        anchor.setAttribute( NAME, text );
        header.appendChild( anchor );
        header.appendChild( textNode );
        parent.appendChild( block );
        
        return block;
    }
    
    /**
     * <p>
     * Create an html text element.
     * </p>
     */
    private Element createTextElement( Document doc, String tag, String text )
        throws Exception
    {
        Element     retval = doc.createElement( tag );
        Text            textNode = doc.createTextNode( text );

        retval.appendChild( textNode );
        
        return retval;
    }

    /**
     * <p>
     * Create a standard link to a local label.
     * </p>
     */
    private Element createLocalLink( Document doc, String anchor, String text )
        throws Exception
    {
        Text        textNode = doc.createTextNode( text );

        return createLocalLink( doc, anchor, textNode );
    }
    
    /**
     * <p>
     * Create a standard link to a local label.
     * </p>
     */
    private Element createLocalLink( Document doc, String anchor, Node visibleText )
        throws Exception
    {
        return createLink( doc, "#" + anchor, visibleText );
    }
    
    /**
     * <p>
     * Create a hotlink.
     * </p>
     */
    private Element createLink( Document doc, String label, String text )
        throws Exception
    {
        Text        textNode = doc.createTextNode( text );

        return createLink( doc, label, textNode );
    }
    
    /**
     * <p>
     * Create a hotlink.
     * </p>
     */
    private Element createLink( Document doc, String label, Node visibleText )
        throws Exception
    {
        Element hotlink = doc.createElement( ANCHOR );

        hotlink.setAttribute( "href", label );
        hotlink.appendChild( visibleText );

        return hotlink;
    }
    
    /**
     * <p>
     * Insert a list at the end of the parent element.
     * </p>
     */
    private Element createList( Element parent )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Element list = doc.createElement( LIST );

        parent.appendChild( list );

        return list;
    }
    
    /**
     * <p>
     * Add an item with a bold name to the end of a list.
     * </p>
     */
    private Element addHeadlinedItem( Element list, String headline, String text )
        throws Exception
    {
        Document        doc = list.getOwnerDocument();
        Element         itemElement = doc.createElement( LIST_ELEMENT );
        Element         boldText = boldText( doc, headline );
        Text            textNode = doc.createTextNode( " - " + text );

        list.appendChild( itemElement );
        itemElement.appendChild( boldText );
        itemElement.appendChild( textNode );

        return itemElement;
    }
    
    /**
     * <p>
     * Make some bold text.
     * </p>
     */
    private Element boldText( Document doc, String text)
        throws Exception
    {
        Element bold = createTextElement( doc, BOLD, text );

        return bold;
    }
    
    /**
     * <p>
     * Add an item to the end of a list.
     * </p>
     */
    private Element addListItem( Element list, Node item )
        throws Exception
    {
        Document        doc = list.getOwnerDocument();
        Element         itemElement = doc.createElement( LIST_ELEMENT );

        list.appendChild( itemElement );
        itemElement.appendChild( item );

        return itemElement;
    }
    
    /**
     * <p>
     * Retrieve the indented block inside a section
     * </p>
     */
    private Element getSection( Document doc, int sectionLevel, String sectionName )
        throws Exception
    {
        String          headerTag = makeHeaderTag( sectionLevel );
        Element     root = doc.getDocumentElement();
        NodeList    sectionList = root.getElementsByTagName( headerTag );
        int             count = sectionList.getLength();

        for ( int i = 0; i < count; i++ )
        {
            Element     section = (Element) sectionList.item( i );
            Element     sectionAnchor = getFirstChild( section, ANCHOR );

            if ( sectionName.equals( sectionAnchor.getAttribute( NAME ) ) )
            {
                // the next item after the section header, is the indented block

                return (Element) section.getNextSibling();
            }
        }

        return null;
    }

    /**
     * <p>
     * Make the tag for a header, given its level
     * </p>
     */
    private String makeHeaderTag( int headerLevel )
    {
        return "h" + Integer.toString( headerLevel );
    }
    
    /**
     * <p>
     * Add a paragraph to the end of a parent element.
     * </p>
     */
    private Element addParagraph( Element parent, String text )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Text            textNode = doc.createTextNode( text );
        Element         paragraph = doc.createElement( PARAGRAPH );

        parent.appendChild( paragraph );
        paragraph.appendChild( textNode );
        
        return paragraph;
    }
    
    /**
     * <p>
     * Insert a table at the end of the parent element.
     * </p>
     */
    private Element createTable( Element parent, int borderWidth, String[] columnHeadings )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Element         table = doc.createElement( TABLE );
        Element         headingRow = insertRow( table );
        int                     count = columnHeadings.length;

        parent.appendChild( table );
        table.setAttribute( BORDER, Integer.toString( borderWidth ) );

        for ( int i = 0; i < count; i++ )
        {
            Element     headingColumn = insertColumn( headingRow );
            Element     boldText = boldText( doc, columnHeadings[ i ] );

            headingColumn.appendChild( boldText );
        }

        return table;
    }
    
    /**
     * <p>
     * Insert a row at the end of a table
     * </p>
     */
    private Element insertRow( Element table )
        throws Exception
    {
        Document        doc = table.getOwnerDocument();
        Element         row = doc.createElement( ROW );

        table.appendChild( row );

        return row;
    }
    
    /**
     * <p>
     * Insert a column at the end of a row
     * </p>
     */
    private Element insertColumn( Element row )
        throws Exception
    {
        Document        doc = row.getOwnerDocument();
        Element         column = doc.createElement( COLUMN );

        row.appendChild( column );

        return column;
    }
    
    /**
     * <p>
     * Insert a horizontal line at the end of the parent element.
     * </p>
     */
    private Element insertLine( Element parent )
        throws Exception
    {
        Document        doc = parent.getOwnerDocument();
        Element         line = doc.createElement( HORIZONTAL_LINE );

        parent.appendChild( line );

        return line;
    }

    ////////////////////////////////////////////////////////
    //
    // XML MINIONS
    //
    ////////////////////////////////////////////////////////

    private Element getFirstChild( Element node, String childName )
        throws Exception
    {
        Element retval = getOptionalChild( node, childName );

        if ( retval == null )
        {
            throw new BuildException( "Could not find child element '" + childName + "' in parent element '" + node.getNodeName() + "'." );
        }

        return retval;
    }

    private Element getNextChild( Element node, String childName, int index )
    throws Exception
    {
    	Element retval = (Element) node.getElementsByTagName( childName ).item( index );

    	if ( retval == null )
    	{
    		throw new BuildException( "Could not find child element '" + childName + "' in parent element '" + node.getNodeName() + "'." );
    	}

    	return retval;
    }
    
    private Element getOptionalChild( Element node, String childName )
        throws Exception
    {
        return (Element) node.getElementsByTagName( childName ).item( 0 );
    }

    /**
     * <p>
     * Squeeze the text out of an Element.
     * </p>
     */
    private String squeezeText( Element node )
        throws Exception
    {
        Node        textChild = node.getFirstChild();
        String      text = textChild.getNodeValue();

        return text;
    }

    /**
     * <p>
     * Clone all of the children of a source node and add them as children
     * of a target node.
     * </p>
     */
    private void cloneChildren( Node source, Node target )
        throws Exception
    {
        Document    targetDoc = target.getOwnerDocument();
        NodeList        sourceChildren = source.getChildNodes();
        int                 count = sourceChildren.getLength();

        for ( int i = 0; i < count; i++ )
        {
            Node    sourceChild = sourceChildren.item( i );
            Node    targetChild = targetDoc.importNode( sourceChild, true );

            target.appendChild( targetChild );
        }
    }

    /**
     * <p>
     * Replace all instances of the tag with the indicated text.
     * </p>
     */
    private void replaceTag( Document doc, String tag, String replacementText )
        throws Exception
    {
        NodeList        sourceChildren = doc.getElementsByTagName( tag );
        int                 count = sourceChildren.getLength();

        for ( int i = 0; i < count; i++ )
        {
            Node    oldChild = sourceChildren.item( i );
            Node    newChild = doc.createTextNode( replacementText );

            if ( oldChild != null )
            {
                Node    parent = oldChild.getParentNode();
            
                if ( parent != null ) { parent.insertBefore( newChild, oldChild ); }
            }
        }

        for ( int i = count-1; i > -1; i-- )
        {
            Node    oldChild = sourceChildren.item( i );

            if ( oldChild != null )
            {
                Node    parent = oldChild.getParentNode();
            
                if ( parent != null ) { parent.removeChild( oldChild ); }
            }
        }
    }

    ////////////////////////////////////////////////////////
    //
    // JIRA MINIONS
    //
    ////////////////////////////////////////////////////////

    /**
     * <p>
     * Get an array of JiraIssues from a JIRA report.
     * </p>
     */
    private JiraIssue[]   getJiraIssues( Document report )
        throws Exception
    {
        Element         reportRoot = report.getDocumentElement();
        NodeList        itemList = reportRoot.getElementsByTagName( JIRA_ITEM );
        int                 count = itemList.getLength();
        JiraIssue[]     issues = new JiraIssue[ count ];

        
        
        //for ( int i = 0; i < count; i++ ) { issues[ i ] = makeJiraIssue( (Element) itemList.item( i ) ); }
        for ( int i = 0; i < count; i++ ) 
        { 
        	JiraIssue ji =  makeJiraIssue( (Element) itemList.item( i ) ); 
        	if (ji != null)
        	{
        		issues[ i ] = ji; 
        	}
        }             

        return issues;
    }

    /**
     * <p>
     * Create a JiraIssue from an <item> element in a JIRA report.
     * </p>
     */
    private JiraIssue   makeJiraIssue( Element itemElement )
        throws Exception
    {
        String  key = squeezeText( getFirstChild( itemElement, JIRA_KEY ) );
        String  title = squeezeText( getFirstChild( itemElement, JIRA_TITLE ) );

        //
        // A JIRA title has the following form:
        //
        //  "[DERBY-2598] new upgrade  test failures after change 528033"
        //
        // We strip off the leading JIRA id because that information already
        // lives in the key.
        //
        title = title.substring( title.indexOf( ']' ) + 2, title.length() );        

        //return new JiraIssue( key, title, releaseNoteAttachmentID );
        String type=squeezeText(getFirstChild(itemElement, "type"));

        try {
        	String resolution=squeezeText(getOptionalChild(itemElement, "resolution"));
        	if (resolution != null)
        	{   
        		if (!resolution.equals("Fixed"))
        			return null;
        	}
        } catch (Exception e) {
        	return null;
        }

        String fixVersion=null;
        try {
        	fixVersion=squeezeText(getOptionalChild(itemElement, JIRA_FIXIN));
        } catch (Exception e) {
        	return null;
        }

        while (fixVersion != null)
        {

        	NodeList        subItemList = itemElement.getElementsByTagName( JIRA_FIXIN );
        	int             fixcount = subItemList.getLength();
        	for (int i=0; i<fixcount; i++)
        	{
        		// if we encounter the previous release's ID, we can do away with entire issue
        		if (_previousReleaseID.equals(fixVersion))
        		{
        			return null;
        		}
        		// hardcode excluding 10.2.1.6; we want to exclude those also
        		if (fixVersion.equals("10.2.1.6"))
        		{
        			return null;
        		}
        		// if the first fixVersion does not match the current release, or the beta,
        		// check the next one
        		if ( (!_releaseID.equals(fixVersion)) &&
        				(!"10.3.0.0".equals(fixVersion)) &&
        				(!"10.3.1.1".equals(fixVersion)) &&
        				(!"10.3.1.2".equals(fixVersion)) &&
        				(!"10.3.1.3".equals(fixVersion))) {
        			try {
        				fixVersion = squeezeText(getNextChild( itemElement, JIRA_FIXIN, i+1));
        				continue;
        			} catch (Exception e) {
        				fixVersion=null;
        				return null;
        			}
        		}
        	}
        	return new JiraIssue( key, title );
        }
        return null;
    }
    
    ////////////////////////////////////////////////////////
    //
    // EXCEPTION PROCESSING MINIONS
    //
    ////////////////////////////////////////////////////////

    /**
     * <p>
     * Special processing for special exceptions.
     * </p>
     */
    private  void    processThrowable( Throwable t )
    {
        if ( t instanceof java.net.UnknownHostException )
        {
            println( "Unknown host '" + t.getMessage() + "'. Can you ping this host from a shell window?" );
        }
    }

    /**
     * <p>
     * Format an error for later reporting.
     * </p>
     */
    private  String    formatError( String text, Throwable t )
    {
        text = text + ": " + t.toString() + "\n" + stringifyStackTrace( t );

        return text;
    }

    /**
     * <p>
     * Print a stack trace as a string.
     * </p>
     */
    private  String stringifyStackTrace( Throwable t )
    {
        StringWriter    sw = new StringWriter();
        PrintWriter     pw = new PrintWriter( sw, true );

        t.printStackTrace( pw );
        pw.flush();
        sw.flush();

        return sw.toString();        
    }

     ////////////////////////////////////////////////////////
    //
    // MISC MINIONS
    //
    ////////////////////////////////////////////////////////

    private  void    println( String text )
    {
        if ( _invokedByAnt )
        {
            log( text, Project.MSG_WARN );
        }
        else
        {
            System.out.println( text );
        }
    }
    
}


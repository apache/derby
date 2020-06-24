/*  Derby - Class org.apache.derbyBuild.ReleaseNotesTransformer

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.derbyBuild;

import java.io.*;
import java.util.*;
import java.text.MessageFormat;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

/**
 * <p>
 * This is an ant task which transforms the Derby release notes into a form
 * which can be digested by the Forrest tool and published on the Derby
 * download site. This involves the following transformations:
 * </p>
 *
 * <ul>
 * <li><b>Remove blockquotes</b> - Forrest silently swallows blockquoted text.</li>
 * <li><b>Remove TOC</b> - Forrest adds its own table of contents and transforms the original TOC into a block of dead links.</li>
 * <li><b>Remove mini TOC</b> - Forrest also transforms the mini TOC in the Issues section into a block of dead links.</li>
 * </ul>
 *
 * <p>
 * In addition, this task adds a pointer to the download page to src/documentation/conf/cli.xconf. This causes
 * the site-building scripts to pull the download page into the build.
 * </p>
 *
 */
public class ReleaseNotesTransformer extends Task
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    private static final String PREAMBLE =
        "<!--\n" +
        "  Licensed to the Apache Software Foundation (ASF) under one or more\n" +
        "  contributor license agreements.  See the NOTICE file distributed with\n" +
        "  this work for additional information regarding copyright ownership.\n" +
        "  The ASF licenses this file to you under the Apache License, Version 2.0\n" +
        "  (the \"License\"); you may not use this file except in compliance with\n" +
        "  the License.  You may obtain a copy of the License at\n" +
        "\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-7010
        "      https://www.apache.org/licenses/LICENSE-2.0\n" +
        "\n" +
        "  Unless required by applicable law or agreed to in writing, software\n" +
        "  distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        "  See the License for the specific language governing permissions and\n" +
        "  limitations under the License.\n" +
        "-->\n" +
        "<html>\n" +
        "<title>Apache Derby {0} Release</title>\n" +
        "<body>\n" +
        "\n" +
        "    <h1>Distributions</h1>\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-7010
        "    <p>Use the links below to download a distribution of Apache Derby. You should <b>always</b> <a href=\"#Verifying+Releases\">verify the integrity</a>\n" +
        "       of distribution files downloaded from a mirror.</p>\n" +
        "\n" +
        "<p>You are currently using <strong>[preferred]</strong>. If you encounter a\n" +
        "problem with this mirror, then please select another.  If all\n" +
        "mirrors are failing, there are backup mirrors at the end of the list.\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-7010
        "See <a href=\"https://www.apache.org/mirrors/\">status</a> of mirrors.\n" +
        "</p>\n" +
        "\n" +
        "<form action=\"[location]\" method=\"get\" id=\"SelectMirror\">\n" +
        "Other mirrors: <select name=\"Preferred\">\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-6875
        "[if-any http] [for http]\n" +
        "<option value=\"[http]\">[http]</option>\n" +
        "[end] [end]\n" +
        "[if-any ftp] [for ftp]\n" +
        "<option value=\"[ftp]\">[ftp]</option>\n" +
        "[end] [end]\n" +
        "[if-any backup] [for backup]\n" +
        "<option value=\"[backup]\">[backup] (backup)</option>\n" +
        "[end] [end]\n" +
        "</select>\n" +
        "<input type=\"submit\" value=\"Change\" />     \n" +
        "</form>\n" +
        "\n" +
        "\n" +
        "    <p>There are four different distributions:</p>\n" +
        "    <ul>\n" +
        "      <li>bin distribution - contains the documentation, javadoc, and jar files for Derby.</li>\n" +
        "      <li>lib distribution - contains only the jar files for Derby.</li>\n" +
        "      <li>lib-debug distribution - contains jar files for Derby with source line numbers.</li>\n" +
        "      <li>src distribution - contains the Derby source tree at the point which the binaries were built.</li>\n" +
        "    </ul>\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-7010
        "    <p> <a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-bin.zip\">db-derby-{0}-bin.zip</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-bin.zip.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-bin.zip.sha512\">SHA-512</a>]<br/>\n" +
        "    <a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-bin.tar.gz\">db-derby-{0}-bin.tar.gz</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-bin.tar.gz.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-bin.tar.gz.sha512\">SHA-512</a>]</p>\n" +
        "    \n" +
        "    <p><a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-lib.zip\">db-derby-{0}-lib.zip</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib.zip.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib.zip.sha512\">SHA-512</a>]<br/>\n" +
        "    <a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-lib.tar.gz\">db-derby-{0}-lib.tar.gz</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib.tar.gz.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib.tar.gz.sha512\">SHA-512</a>]</p>\n" +
        "    \n" +
        "    <p><a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.zip\">db-derby-{0}-lib-debug.zip</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.zip.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.zip.sha512\">SHA-512</a>]<br/>\n" +
        "    <a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.tar.gz\">db-derby-{0}-lib-debug.tar.gz</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.tar.gz.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-lib-debug.tar.gz.sha512\">SHA-512</a>]</p>\n" +
        "\n" +
        "    <p><a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-src.zip\">db-derby-{0}-src.zip</a>  [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-src.zip.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-src.zip.sha512\">SHA-512</a>]<br/>\n" +
        "    <a href=\"[preferred]/db/derby/db-derby-{0}/db-derby-{0}-src.tar.gz\">db-derby-{0}-src.tar.gz</a> [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-src.tar.gz.asc\">PGP</a>] [<a href=\"https://www.apache.org/dist/db/derby/db-derby-{0}/db-derby-{0}-src.tar.gz.sha512\">SHA-512</a>] (Note that, due to long filenames, you will need gnu tar to unravel this tarball.)</p>\n";
    


    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    private DocumentBuilder _docBldr;
    private Document _inputDoc;
    private File _inputFile;
    private File _outputFile;
    private File _cliXconfFile;

    private String _inputFileName;
    private String _outputFileName;
    private String _cliXconfFileName;
    private String _releaseID;

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////////////

    public ReleaseNotesTransformer() throws Exception
    {
        _docBldr = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  ANT Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////
    
    /**
     * Ant accessor to set the name of the input file, the original release notes.
     */
    public void setInputFileName(String inputFileName) throws Exception
    {
        _inputFileName = inputFileName;
        _inputFile = new File(_inputFileName);

        println( "Reading from " + inputFileName + "..." );
    }

    /**
     * Ant accessor to set the name of the generated output file
     */
    public void setOutputFileName(String outputFileName) throws Exception
    {
        _outputFileName = outputFileName;
        _outputFile = new File(_outputFileName);

        println( "Writing to " + outputFileName + "..." );
    }

    /**
     * Ant accessor to set the name of the cli.xconf file which pulls the download page
     * into the built site.
     */
    public void setCliXconfFileName(String cliXconfFileName) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4855
        _cliXconfFileName = cliXconfFileName;
        _cliXconfFile = new File(_cliXconfFileName);

        println( "Writing import instructions to to " + cliXconfFileName + "..." );
    }

    /**
     * Ant accessor to set the release id.
     */
    public void setReleaseId(String releaseID) throws Exception
    {
        _releaseID = releaseID;

        println( "Setting release id to " + _releaseID + "..." );
    }

    /**
     * This is Ant's entry point into this task.
     */
    public void execute() throws BuildException
    {
        try {
            transform();
            printOutput();
            postProcess();

//IC see: https://issues.apache.org/jira/browse/DERBY-4855
            wireIntoBuild();
        }
        catch (Throwable t) {
            t.printStackTrace();

            throw new BuildException("Error running ReleaseNotesTransformer: " + t.getMessage(), t);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  CORE BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     *<p>
     * This is the guts of the processing.
     * </p>
     */
    private void transform() throws Exception
    {
        // this writes normalized text to the output file
        normalizeText( _inputFile, _outputFile );

        // correct text so that it is parseable and remove brackets which Forrest can't see
        InputStream normalizedText = new FileInputStream( _outputFile );
        _inputDoc = _docBldr.parse( normalizedText );
        normalizedText.close();
        
        removeBlockquotes();
        removeTopTOC();
        removeIssuesTOC();
    }

    /**
     * <p>
     * Remove the blockquotes which hide text from Forrest.
     * </p>
     */
    private void removeBlockquotes() throws Exception
    {
        Element root = _inputDoc.getDocumentElement();
        HashSet<Element> replacedNodes = new HashSet<Element>();
        String tag = "blockquote";

        while ( true )
        {
            Element suspect = getFirstDescendant( root, tag );
            if ( suspect == null ) { break; }

            if ( replacedNodes.contains( suspect ) )
            {
                throw new Exception( "Stuck in a loop trying to strip '" + tag + "'" );
            }
            replacedNodes.add( suspect );

            Element parent = (Element) suspect.getParentNode();
            NodeList children = suspect.getChildNodes();

            if ( children != null )
            {
                int childCount = children.getLength();

                for ( int i = 0; i < childCount; i++ )
                {
                    Node oldChild = children.item( i );

                    if ( oldChild != null )
                    {
                        Node newChild = oldChild.cloneNode( true );

                        parent.insertBefore( newChild, suspect );
                    }
                }
            }
            parent.removeChild( suspect );
        }   // end loop through suspects
    }
    
    /**
     * <p>
     * Remove the top level table of contents. This is the first list in the document.
     * </p>
     */
    private void removeTopTOC() throws Exception
    {
        removeFirstList( _inputDoc.getDocumentElement() );
    }

    /**
     * <p>
     * Remove the table of contents of the Issues section. This is the first list in that section.
     * </p>
     */
    private void removeIssuesTOC() throws Exception
    {
        Element issuesHeader = findHeader( 2, "Issues" );

        if ( issuesHeader != null )
        {
            // now look for the first list that follows

            NodeList allLists = _inputDoc.getDocumentElement().getElementsByTagName( "ul" );

            if ( allLists == null ) { return; }

            int count = allLists.getLength();

            for ( int i = 0; i < count; i++ )
            {
                Node nextList = allLists.item( i );
                
                if ( issuesHeader.compareDocumentPosition( nextList ) == Node.DOCUMENT_POSITION_FOLLOWING )
                {
                    nextList.getParentNode().removeChild( nextList );
                    break;
                }
            }

        }
    }

    /**
     * <p>
     * Remove the first list under an element.
     * </p>
     */
    private void removeFirstList( Element root ) throws Exception
    {
        Element listElement = getFirstDescendant( root, "ul" );

        if ( listElement != null )
        {
            listElement.getParentNode().removeChild( listElement );
        }
    }

    /**
     * <p>
     * Find the header element with this given name.
     * </p>
     */
    private Element findHeader( int headerLevel, String headerTitle ) throws Exception
    {
        Element root = _inputDoc.getDocumentElement();
        String headerTag = "h" +  headerLevel;
        NodeList headers = root.getElementsByTagName( headerTag );

        if ( headers == null ) { return null; }

        int count = headers.getLength();

        for ( int i = 0; i < count; i++ )
        {
            Node nextHeader = headers.item( i );
            String title = nextHeader.getTextContent().trim();

            if ( headerTitle.equals( title ) ) { return (Element) nextHeader; }
        }

        return null;
    }

    
    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    private Element getFirstDescendant( Element ancestor, String tagName )
    {
        NodeList nl = ancestor.getElementsByTagName( tagName );

        if ( nl == null ) { return null; }
        if ( nl.getLength() == 0 ) { return null; }

        return (Element) nl.item( 0 );
    }
    
    /**
     * Adjust input text to remove junk which confuses the xml parser and/or Forrest.
     * Temporarily writes the adjusted text to the output file.
     */
    private void normalizeText( File inputFile, File outputFile ) throws Exception
    {
        String rawString = readFileIntoString( inputFile );

        // The Transformer which wrote the original release notes managed to turn <br/> into <br>
        // and <hr/> into <hr>. Fix this.
        rawString = fullReplaceToken( rawString, "<br>", "<br/>" );
        rawString = fullReplaceToken( rawString, "<hr>", "<hr/>" );

        // Forrest doesn't like square brackets and swallows the bracketed content
//IC see: https://issues.apache.org/jira/browse/DERBY-5009
        rawString = rawString.replace( '[', '(' );
        rawString = rawString.replace( ']', ')' );

        FileWriter fileWriter = new FileWriter( outputFile );
        fileWriter.append( rawString );
        fileWriter.flush();
        fileWriter.close();
    }
    private String fullReplaceToken( String rawString, String token, String replacement )
    {
        rawString = replaceToken( rawString, token.toLowerCase(), replacement );
        rawString = replaceToken( rawString, token.toUpperCase(), replacement );
        
        return rawString;
    }
    private String replaceToken( String rawString, String token, String replacement )
    {
        StringWriter output = new StringWriter();
        int rawLength = rawString.length();
        int tokenLength = token.length();
        int start = 0;

        while ( true )
        {
            int idx = rawString.indexOf( token, start );
            if ( idx < 0 ) { break; }

            output.append( rawString.substring( start, idx ) );
            output.append( replacement );
            start = idx + tokenLength;
        }

        if ( start < rawLength )
        {
            output.append( rawString.substring( start, rawLength ) );
        }

        return output.toString();
    }
    
    /**
     * Print the generated output document to the output file.
     */
    private void printOutput() throws Exception
    {
        Source source = new DOMSource(_inputDoc);

        Result result = new StreamResult(_outputFile);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();

        transformer.transform(source, result);
    }

    /**
     * <p>
     * Post-process the output:
     * </p>
     *
     * <ul>
     * <li>Add preamble to the head of the file.</li>
     * </ul>
     */
    private void postProcess()
        throws Exception
    {
        String shortReleaseID = _releaseID.substring( 0, _releaseID.lastIndexOf( "." ) );
        String preamble = MessageFormat.format( PREAMBLE, _releaseID, shortReleaseID );
        String contents = readFileIntoString( _outputFile );
        String firstHeader = "<h1>";
        int cutIdx = contents.indexOf( firstHeader );
        String result = preamble + contents.substring( cutIdx );

        writeStringIntoFile( result, _outputFile );
    }
    
    /**
     * <p>
     * Wire the download page into the build instructions.
     * </p>
     */
    private void wireIntoBuild()
//IC see: https://issues.apache.org/jira/browse/DERBY-4855
        throws Exception
    {
        String contents = readFileIntoString( _cliXconfFile );
        int insertPoint = contents.indexOf( "   </uris>" );
        String insertion = "     <uri type=\"append\" src=\"releases/release-" + _releaseID + ".html\"/>\n";
        String result = contents.substring( 0, insertPoint ) + insertion + contents.substring( insertPoint );

        writeStringIntoFile( result, _cliXconfFile );
    }
    
    /**
     * Print a line of text to the console.
     */
    private void println(String text)
    {
        log(text, Project.MSG_WARN);
    }

    /**
     * <p>
     * Read a file and return the entire contents as a single String.
     * </p>
     */
    private String readFileIntoString( File inputFile ) throws Exception
    {
        FileReader fileReader = new FileReader( inputFile );
        StringWriter stringWriter = new StringWriter();

        while( true )
        {
            int nextChar = fileReader.read();
            if ( nextChar < 0 ) { break; }

            stringWriter.append( (char) nextChar );
        }

        String rawString = stringWriter.toString();

        return rawString;
    }

    /**
     * <p>
     * Write a string into a file.
     * </p>
     */
    private void writeStringIntoFile( String rawString, File outputFile )
        throws Exception
    {
        PrintWriter writer = new PrintWriter( outputFile, "UTF-8" );

        writer.println( rawString );
        writer.flush();
        writer.close();
    }

}

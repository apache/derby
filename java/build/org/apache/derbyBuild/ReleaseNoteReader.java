/*

   Derby - Class org.apache.derbyBuild.ReleaseNoteReader

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

/**
 * <p>
 * This tool reads a release note from a stream. When run standalone, this is
 * a file stream. When run from the ReleaseNoteGenerator, the stream is opened
 * on the URL of a release note stored in JIRA. The purpose of this class it to
 * help people verify that their release notes can be digested by the ReleaseNoteGenerator.
 * </p>
 */
public class ReleaseNoteReader
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////
    
    private static  final   String  USAGE =
        "Usage:\n" +
        "\n" +
        "  java org.apache.derbyBuild.ReleaseNoteReader RELEASE_NOTE_FILE\n" +
        "\n" +
        "    where\n" +
        "                  RELEASE_NOTE_FILE is the name of the file which holds the release note\n";

    private static  final   String  PARAGRAPH = "p";
    private static  final   String  BODY = "body";

    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////

    private DocumentBuilder _documentBuilder;

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////

    public ReleaseNoteReader( DocumentBuilder documentBuilder )
    {
        _documentBuilder = documentBuilder;
    }
        
    /////////////////////////////////////////////////////////////////////////
    //
    //  MAIN
    //
    /////////////////////////////////////////////////////////////////////////

   /**
    * The program entry point exercises all of the checks which
    * would be performed by the ReleaseNoteGenerator on this
    * particular release note. Takes one argument, the name of
    * the file which holds the release note.
    */
    public  static void main( String[] args )
        throws Exception
    {
        if ( (args == null) || (args.length != 1) )
        {
            println( USAGE );
            System.exit(1);
        }

        String                                  fileName = args[ 0 ];
        FileInputStream                 fis = new FileInputStream( fileName );
        DocumentBuilderFactory  factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder              builder = factory.newDocumentBuilder();
        ReleaseNoteReader           me = new ReleaseNoteReader( builder );

        // here are the checks we perform
        Document                        doc = me.getReleaseNote( fis );
        Element                           summary = me.getReleaseNoteSummary( doc );
        Element                         details = me.getReleaseNoteDetails( doc );

        // if you get this far, then everything worked

        println( "\n" + fileName + " passes the currently known checks performed by the release note generator.\n" );
    }
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  BEHAVIOR CALLED BY ReleaseNoteGenerator
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the release note for an issue.
     * </p>
     */
    public Document   getReleaseNote( InputStream is )
        throws Exception
    {
        Document        doc = _documentBuilder.parse( is );

        is.close();
        
        return doc;
    }

    /**
     * <p>
     * Get the summary paragraph for a release note
     * </p>
     */
    public Element   getReleaseNoteSummary( Document releaseNote )
        throws Exception
    {
        //
        // The release note has the following structure:
        //
        // <h4>Summary of Change</h4>
        // <p>
        //  Summary text
        // </p>
        //
        Element     root = releaseNote.getDocumentElement();
        Element     summaryParagraph = getFirstChild( root, PARAGRAPH );

        return summaryParagraph;
    }
 
    /**
     * <p>
     * Get the detail section for a release note
     * </p>
     */
    public Element   getReleaseNoteDetails( Document releaseNote )
        throws Exception
    {
        Element     root = releaseNote.getDocumentElement();
        Element     details = getFirstChild( root, BODY );

        return details;
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
            throw new Exception( "Could not find child element '" + childName + "' in parent element '" + node.getNodeName() + "'." );
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

   ////////////////////////////////////////////////////////
    //
    // MISC MINIONS
    //
    ////////////////////////////////////////////////////////

    private  static void    println( String text )
    {
        System.out.println( text );
    }
}

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
import javax.xml.parsers.*;
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

        me.forbidBlockQuotes( doc );

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
        Element     summaryParagraph = GeneratorBase.getFirstChild( root, GeneratorBase.PARAGRAPH );

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
        Element     details = GeneratorBase.getFirstChild( root, GeneratorBase.BODY );

        return details;
    }

    /**
     * <p>
     * Forbid BLOCKQUOTEs for accessibility reasons. See http://www.w3.org/TR/WCAG10/#gl-structure-presentation
     * </p>
     */
    private void    forbidBlockQuotes( Document releaseNote )   throws Exception
    {
        Element     root = releaseNote.getDocumentElement();
        String          errorMessage = "For accessibility reasons, blockquotes are not allowed. Please remove the blockquote tags.";

        forbid( root, "BLOCKQUOTE", errorMessage );
        forbid( root, "blockquote", errorMessage );
    }
    private void    forbid( Element root, String tag, String errorMessage ) throws Exception
    {
        NodeList    tags = root.getElementsByTagName( tag );

        if ( (tags != null) && (tags.getLength() > 0) )
        {
            throw new Exception( errorMessage );
        }
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

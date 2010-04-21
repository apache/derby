/*

   Derby - Class org.apache.derbyBuild.TagReader

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
import java.text.ParseException;

/**
 * <p>
 * This little machine is constructed from an xml/html document/element and is used to parse
 * content inside that element. This machine advances through the element,
 * letting the operator look for substrings. This machine was created to read
 * elements in JIRA's html reports, which are not well-formed xml documents.
 * </p>
 *
 * <p>
 * To operate the TagReader, keep positioning on substrings, following some
 * pattern which allows you to navigate to the content you need.
 * </p>
 */
public class TagReader
{
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    private static final int NOT_FOUND = -1;
    
    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////f

    private String _content;
    private int _cursor;

    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////

    /** Wrap a TagReader around a piece of content */
    public TagReader( String content )
    {
        if ( content == null ) { content = ""; }
        
        _content = content;

        init();
    }

    /** Wrap a TagReader around the content siphoned out of a stream */
    public TagReader( InputStream is ) throws IOException
    {
        StringWriter buffer = new StringWriter();

        while( true )
        {
            int nextChar = is.read();
            if ( nextChar < 0 ) { break; }

            buffer.write( nextChar );
        }

        _content = buffer.toString();

        is.close();

        init();
    }

    /** Initialization common to all constructors */
    private void init()
    {
        reset();
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  PUBLIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Resets the reader to the beginning of the content.
     * </p>
     */
    public void reset()
    {
        _cursor = 0;
    }
    
    /**
     * <p>
     * Starting at the current position, search for a substring in the content. If the substring is found, positions
     * the reader AFTER the substring and returns that new cursor position. If the
     * substring is not found, does not advance the cursor, but returns -1.
     * </p>
     */
    public int position( String tag, boolean failIfNotFound ) throws ParseException
    {
        int retval = NOT_FOUND;
        
        if ( _cursor < _content.length() )
        {
            retval = _content.indexOf( tag, _cursor );

            if ( retval < 0 ) { retval = NOT_FOUND; }
            else
            {
                retval += tag.length();
                _cursor = retval;
            }
        }

        if ( failIfNotFound && ( retval == NOT_FOUND ) )
        {
            throw new ParseException( "Could not find substring '" + tag + "'", _cursor );
        }

        return retval;
    }

    /**
     * <p>
     * Starting at the current position, search for a substring in the content. If the
     * substring is found, return everything from the cursor up to the start of the substring
     * and position the reader AFTER the substring. If the substring is not found, return null
     * and do not alter the cursor.
     * </p>
     */
    public String getUpTill( String tag, boolean failIfNotFound ) throws ParseException
    {
        int oldCursor = _cursor;
        int endIdx = position( tag, failIfNotFound );

        if ( endIdx < 0 ) { return null; }

        return _content.substring( oldCursor, endIdx - tag.length() );
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////


}

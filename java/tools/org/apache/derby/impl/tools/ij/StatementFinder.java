/*

   Derby - Class org.apache.derby.impl.tools.ij.StatementFinder

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

package org.apache.derby.impl.tools.ij;

import java.io.IOException;
import java.io.Reader;

/**
	StatementGrabber looks through an input stream for
	the next JSQL statement.  A statement is considered to
	be any tokens up to the next semicolon or EOF.
	<p>
	Semicolons inside comments, strings, and delimited identifiers
	are not considered to be statement terminators but to be
	part of those tokens.
	<p>
	The only comment form currently recognized is the SQL comment,
	which begins with "--" and ends at the next EOL.
	<p>
	Strings and delimited identifiers are permitted to contain
	newlines; the actual IJ or JSQL parsers will report errors when
	those cases occur.
	<p>
	There are no escaped characters, i.e. "\n" is considered to
	be two characters, '\' and 'n'.

	@author ames
 */

public class StatementFinder {

	private Reader source; 
	private StringBuffer statement = new StringBuffer();
	private int state;
	private boolean atEOF = false;
	private boolean peekEOF = false;
	private char peekChar;
	private boolean peeked = false;

	// state variables
	private static final int IN_STATEMENT = 0;
	private static final int IN_STRING = 1;
	private static final int IN_SQLCOMMENT = 2;
	private static final int END_OF_STATEMENT = 3;
	private static final int END_OF_INPUT = 4;

	// special state-changing characters
	private static final char MINUS = '-';
	private static final char SINGLEQUOTE = '\'';
	private static final char DOUBLEQUOTE = '\"';
	private static final char SEMICOLON = ';';
	private static final char NEWLINE = '\n';
	private static final char RETURN = '\r';
	private static final char SPACE = ' ';
	private static final char TAB = '\t';
	private static final char FORMFEED = '\f';

	/**
		The constructor does not assume the stream is data input
		or buffered, so it will wrap it appropriately.

		@param s the input stream for reading statements from.
	 */
	public StatementFinder(Reader s) { 
		source = s;
	}

	/**
		Reinit is used to redirect the finder to another stream.
		The previous stream should not have been in a PEEK state.

		@param s the input stream for reading statements from.
	 */
	public void ReInit(Reader s) { 
	    try {
			source.close();
		} catch (IOException ioe) {
			// just be quiet if it is already gone
		}
		source = s;
		state = IN_STATEMENT;
		atEOF = false;
		peekEOF = false;
		peeked = false;
	}

	public void close() throws IOException {
		source.close();
	}

	/**
		get the next statement in the input stream. Returns it,
		dropping its closing semicolon if it has one. If there is
		no next statement, return a null.

		@return the next statement in the input stream.
	 */
	public String nextStatement() {
		boolean haveSemi = false;
		char nextChar;

		// initialize fields for getting the next statement
		statement.setLength(0);
		if (state == END_OF_INPUT) return null;

		state = IN_STATEMENT;

		// skip leading whitespace
		nextChar = peekChar();
		if (peekEOF()) {
			state = END_OF_INPUT;
			return null;
		}
		if (whiteSpace(nextChar)) {
			while (whiteSpace(peekChar()) && ! peekEOF());
			if (peekEOF()) {
				state = END_OF_INPUT;
				return null;
			}
		}

		while (state != END_OF_STATEMENT && state != END_OF_INPUT) {

			// get the next character from the input
			nextChar = readChar();
			if (atEOF()) {
				state = END_OF_INPUT;
				break;
			}

			switch(nextChar) {
				case MINUS:
					readSingleLineComment(nextChar);
					break;
				case SINGLEQUOTE:
				case DOUBLEQUOTE:
					readString(nextChar);
					break;
				case SEMICOLON:
					haveSemi = true;
					state = END_OF_STATEMENT;
					break;
				default:
					// keep going, just a normal character
					break;
			}
		}

		if (haveSemi)
			statement.setLength(statement.length()-1);
		return statement.toString();
	}

	/**
		Determine if the given character is considered whitespace

		@param c the character to consider
		@return true if the character is whitespace
	 */
	private boolean whiteSpace(char c) {
		return (c == SPACE ||
		    	c == TAB ||
		    	c == RETURN ||
		    	c == NEWLINE ||
		    	c == FORMFEED);
	}

	/**
		Advance the source stream to the end of a comment if it
		is on one, assuming the first character of
		a potential single line comment has been found.
		If it is not a comment, do not advance the stream.
		<p>
		The form of a single line comment is, in regexp, XX.*$,
		where XX is two instances of commentChar.

		@param commentChar the character whose duplication signifies
			the start of the comment.
	 */
	private void readSingleLineComment(char commentChar) {
		char nextChar;

		nextChar = peekChar();
		// if next char is EOF, we are done.
		if (peekEOF()) return;

		// if nextChar is not a minus, it was just a normal minus,
		// nothing special to do
		if (nextChar != commentChar) return;

		// we are really in a comment
		readChar(); // grab the minus for real.

		state = IN_SQLCOMMENT;
		do {
			nextChar = peekChar();
			if (peekEOF()) {
				// let the caller process the EOF, don't read it
				state = IN_STATEMENT;
				return;
			}
			switch (nextChar) {
				case NEWLINE:
				case RETURN:
					readChar(); // okay to process the character
					state = IN_STATEMENT;
					return;
				default:
					readChar(); // process the character, still in comment
					break;
			}
		} while (state == IN_SQLCOMMENT); // could be while true...
	}

	/**
		Advance the stream to the end of the string.
		Assumes the opening delimiter of the string has been read.
		This handles the SQL ability to put the delimiter within
		the string by doubling it, by reading those as two strings
		sitting next to one another.  I.e, 'Mary''s lamb' is read
		by this class as two strings, 'Mary' and 's lamb'.
		<p>
		The delimiter of the string is expected to be repeated at
		its other end. If the other flavor of delimiter occurs within
		the string, it is just a normal character within it.
		<p>
		All characters except the delimiter are permitted within the
		string. If EOF is hit before the closing delimiter is found,
		the end of the string is assumed. Parsers using this parser
		will detect the error in that case and return appropriate messages.

		@param stringDelimiter the starting and ending character
			for the string being read.
	 */
	private void readString(char stringDelimiter) {
		state = IN_STRING;
		do {
			char nextChar = readChar();

			if (atEOF()) {
				state = END_OF_INPUT;
				return;
			}

			if (nextChar == stringDelimiter) {
				// we've reached the end of the string
				state = IN_STATEMENT;
				return;
			}

			// still in string
		} while (state == IN_STRING); // could be while true...
	}

	private boolean atEOF() {
		return atEOF;
	}

	private boolean peekEOF() {
		return peekEOF;
	}

	/**
		return the next character in the source stream and
		append it to the statement buffer.

		@return the next character in the source stream.
	 */
	private char readChar() {
		if (!peeked) peekChar();

		peeked = false;
		atEOF = peekEOF;

		if (!atEOF) statement.append(peekChar);

		return peekChar;
	}

	/**
		return the next character in the source stream, without
		advancing.

		@return the next character in the source stream.
	 */
	private char peekChar() {
		peeked = true;
		char c = '\00';

		try {
		    int cInt;

			// REMIND: this is assuming a flat ascii source file.
			// will need to beef it up at some future point to
			// understand whether the stream is ascii or something else.
			cInt = source.read();
			peekEOF = (cInt == -1);
			if (!peekEOF) c = (char)cInt;
		} catch (IOException ie) {
			throw ijException.iOException(ie);
		}

		peekChar = c;
		return c;
	}
}

/*

   Derby - Class org.apache.derby.iapi.util.IdUtil

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.util;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.error.StandardException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Vector;
import java.util.HashSet;
import java.util.Properties;
import org.apache.derby.iapi.reference.Limits;

/**
  Utility class for parsing and producing string representations of
  ids. This class supports both delimited and un-delimited ids.

  <P>The syntax for an id follows. 
  <PRE>
      id := delim-id | unDelim-id

	  delim-id := "[""|[any char but quote]]+"
	  undelim-id := (a-z|A-Z|anyunicodeletter)[a-z|A-Z|_|0-9|anyunicodeletter|anyunicodedigit]*

	  In the syntax braces show grouping. '*' means repeat 0 or more times.
	  '|' means or. '+' means repeat 1 or more times. 
  </PRE>

  <P>In addition this class provides support for qualified names. A qualified name
  is a dot (.) separated list of ids.

  <P>Limitations:
  <OL>
  <LI>Unicode escape sequences in ids are not supported.
  <LI>Escape sequences (\n...) are not supported.
  </OL>
  */
public abstract class IdUtil
{
	/**
     * Produce a delimited form of a normal value.
	  @return the delimited identifier.
	  */
	public static String normalToDelimited(String id)
	{
        return StringUtil.quoteString(id, '"');
	}

	/**
	  Produce a delimited two part qualified name from two
	  un-delimited identifiers.
	  @return the result.
	  */
	public static String mkQualifiedName(String id1,
										 String id2)
	{
        if( null == id1)
            return normalToDelimited(id2);
		return
        normalToDelimited(id1) +
			"." +
            normalToDelimited(id2);
	}

	/**
	  Make a string form of a qualified name from the array of ids provided.
	  */
	public static String mkQualifiedName(String[] ids)
	{
		StringBuffer sb = new StringBuffer();
		for (int ix=0; ix < ids.length; ix++)
		{
			if (ix!=0) sb.append(".");
			sb.append(normalToDelimited(ids[ix]));
		}
		return sb.toString();
	}

	/**
	  Parse a multi-part (dot separated) SQL identifier form the
      String provided. Raise an excepion
	  if the string does not contain valid SQL indentifiers.
      The returned String array contains the normalized form of the
      identifiers.
      
      @param s The string to be parsed
      @return An array of strings made by breaking the input string at its dots, '.'.
	  @exception StandardException Oops
	  */
	public static String[] parseMultiPartSQLIdentifier(String s)
		 throws StandardException
	{
		StringReader r = new StringReader(s);
		String[] qName = parseMultiPartSQLIdentifier(r);
		verifyEmpty(r);
		return qName;
	}

    /**
    @param r The multi-part identifier to be parsed
    @return An array of strings made by breaking the input string at its dots, '.'.
      @exception StandardException Oops
      */
	private static String[] parseMultiPartSQLIdentifier(StringReader r)
		 throws StandardException
	{
		Vector<String> v = new Vector<String>();
		while (true)
		{
			String thisId = parseId(r,true);
			v.add(thisId);
			int dot;

			try {
				r.mark(0);
				dot = r.read();
				if (dot != '.')
				{
					if (dot!=-1) r.reset();
					break;
				}
			}

			catch (IOException ioe){
				throw StandardException.newException(SQLState.ID_PARSE_ERROR,ioe);
			}
		}
		String[] result = new String[v.size()];
		v.copyInto(result);
		return result;
	}
	
	/**
      Parse a SQL identifier from the String provided. Raise an excepion
      if the string does not contain a valid SQL indentifier.
      The returned String  contains the normalized form of the
      identifier.
        
	  @exception StandardException Oops
	  */
	public static String parseSQLIdentifier(String s)
		 throws StandardException
	{
		StringReader r = new StringReader(s);
		String id = parseId(r,true);
		verifyEmpty(r);
		return id;
	}

	/**
	  Read an id from the StringReader provided.


	  @param normalize true means return ids in nomral form, false means
	        return them as they were entered.

	  <P>
	  Raise an exception if the first thing in the StringReader
	  is not a valid id.

	  @exception StandardException Ooops.
	  */
	private static String parseId(StringReader r, boolean normalize)
		 throws StandardException
	{
		try {
			r.mark(0);
			int c = r.read();
			if (c == -1)  //id can't be 0-length
				throw StandardException.newException(SQLState.ID_PARSE_ERROR);
 			r.reset();
			if (c == '"')
				return parseQId(r,normalize);
			else
				return parseUnQId(r,normalize);
		}

		catch (IOException ioe){
			throw StandardException.newException(SQLState.ID_PARSE_ERROR,ioe);
		}
	}


	/**
	 * Given a case normal form SQL authorization identifier, convert it to a
	 * form that may be compared with the username of Derby builtin
	 * authentication, which uses Java properties of the form
	 * {@code derby.user.}&lt;username&gt;.
	 * <p>
	 * The returned form is suitable for comparing against the property string,
	 * cf.  {@code systemPropertiesExistsBuiltinUser}.
	 * <p>
	 * E.g.:
	 * <p>
	 * <pre>
	 *  Argument -&gt; Return
	 *  ------------------
	 *  EVE      -&gt; eve       [will match Java property: derby.user.eve]
	 *  eVe      -&gt; "eVe"     [will match Java property: derby.user."eVe"]
	 *  "eve"    -&gt; """eve""" [will match Java property: derby.user."""eVe"""]
	 *  \eve\    -&gt; "\eve\"   [will match Java property: derby.user."\eve\"]
	 *
	 * The latter could look this if specified on a Unix shell command line:
	 *
	 *                      -Dderby.user.'"\eve\"'=&lt;password&gt;
	 *
	 * Note: The processing of properties specified on the command line do not
	 * interpret backslash as escape in the way done by the
	 * java.util.Properties#load method, so no extra backslash is needed above.
	 *
	 * </pre>
	 * Since parseSQLIdentifier maps many-to-one, the backward mapping is
	 * non-unique, so the chosen lower case canonical form is arbitrary,
	 * e.g. we will not be able to correctly match the non-canonical:
	 * <p>
     * <pre>
	 *                      [Java property: derby.user.eVe]
	 * </pre>
	 * since this is internally EVE (but see DERBY-3150), and maps back as eve
	 * after the rules above.
	 * @see org.apache.derby.iapi.services.property.PropertyUtil#propertiesContainsBuiltinUser
	 * @see org.apache.derby.iapi.services.property.PropertyUtil#systemPropertiesExistsBuiltinUser
	 */
	public static String SQLIdentifier2CanonicalPropertyUsername(String authid){
		boolean needsQuote = false;
		String result;

		for (int i=0; i < authid.length(); i++) {
			char c = authid.charAt(i);
			// The only external form that needs no quoting contains
			// only uppercase ASCII, underscore, and if not the first
			// character, a decimal number. In all other cases, we
			// envelop in double quotes.
			if (!( (c >= 'A' && c <= 'Z') ||
				   (c == '_') ||
				   (i > 0 && (c >= '0' && c <= '9')))) {
				needsQuote = true;
				break;
			}
		}

		if (!needsQuote) {
			result = authid.toLowerCase();
		} else {
			result = normalToDelimited(authid);
		}

		return result;
	}

    /**
     * Parse a regular identifier (unquoted) returning returning either
     * the value of the identifier or a delimited identifier. Ensures
     * that all characters in the identifer are valid for a regular identifier.
     * 
     * @param r Regular identifier to parse.
     * @param normalize If true return the identifer converted to a single case, otherwise return the identifier as entered.
     * @return the value of the identifer or a delimited identifier
     * @throws IOException Error accessing value
     * @throws StandardException Error parsing identifier.
 
     */
	private static String parseUnQId(StringReader r, boolean normalize)
		 throws IOException,StandardException
	{
		StringBuffer b = new StringBuffer();
		int c;
		boolean first;
		//
		for(first = true; ; first=false)
		{
			r.mark(0);
			if (idChar(first,c=r.read()))
				b.append((char)c);
			else
				break;
		}
		if (c != -1) r.reset();
        
        String id = b.toString();

		if (normalize)
			return StringUtil.SQLToUpperCase(id);
		else
			return id;
	}


	private static boolean idChar(boolean first,int c)
	{
		if (((c>='a' && c<='z') || (c>='A' && c<='Z')) ||
			(!first &&(c>='0' && c<='9')) || (!first &&c =='_') )
			return true;
		else if (Character.isLetter((char) c))
			return true;
		else if (!first && Character.isDigit((char) c))
			return true;
		return false;
	}
    
    /**
     * Parse a delimited (quoted) identifier returning either
     * the value of the identifier or a delimited identifier.
     * @param r Quoted identifier to parse.
     * @param normalize If true return a delimited identifer, otherwise return the identifier's value.
     * @return the value of the identifer or a delimited identifier
     * @throws IOException Error accessing value
     * @throws StandardException Error parsing identifier.
     */
	private static String parseQId(StringReader r,boolean normalize)
		 throws IOException,StandardException
	{
		StringBuffer b = new StringBuffer();
		int c = r.read();
		if (c != '"') throw StandardException.newException(SQLState.ID_PARSE_ERROR);
		while (true)
		{
			c=r.read();
			if (c == '"')
			{
				r.mark(0);
				int c2 = r.read();
				if (c2 != '"')
				{
					if (c2!=-1)r.reset();
					break;
				}
			}
			else if (c == -1)
				throw StandardException.newException(SQLState.ID_PARSE_ERROR);
			
			b.append((char)c);
		}

		if (b.length() == 0) //id can't be 0-length
			throw StandardException.newException(SQLState.ID_PARSE_ERROR);

		if (normalize)
			return b.toString();
		else
			return normalToDelimited(b.toString()); //Put the quotes back.
	}

    /**
     * Verify the read is empty (no more characters in its stream).
     * @param r
     * @throws StandardException
     */
	private static void verifyEmpty(java.io.Reader r)
		 throws StandardException
	{
		try {
			if (r.read() != -1)
				throw StandardException.newException(SQLState.ID_PARSE_ERROR);
		}

		catch (IOException ioe){
			throw StandardException.newException(SQLState.ID_PARSE_ERROR,ioe);
		}			
	}
	/**Index of the schema name in a jar name on a db classpath*/
	public static final int DBCP_SCHEMA_NAME = 0;
	/**Index of the sql jar name in a jar name on a db classpath*/
	public static final int DBCP_SQL_JAR_NAME = 1;
	
	/**
	  Scan a database classpath from the string provided. This returns
	  an array with one qualified name per entry on the classpath. The
	  constants above describe the content of the returned names. This 
	  raises an an exception if the string does not contain a valid database 
	  class path.
  <PRE>
      classpath := item[:item]*
	  item := id.id
	  
	  In the syntax braces ([]) show grouping. '*' means repeat 0 or more times.
	  The syntax for id is defined in IdUtil.
  </PRE>
	  <BR>
	  Classpath returned is a two part name.	  <BR>
	  If the class path is empty then this returns an array
	  of zero length.

	  @exception StandardException Oops
	  */
	public static String[][] parseDbClassPath(String input)
		 throws StandardException
	{
		//As a special case we accept a zero length dbclasspath.
		if (input.length() == 0)
			return new String[0][];

		Vector<String[]> v = new Vector<String[]>();
		java.io.StringReader r = new java.io.StringReader(input);
		//
		while (true)
		{
			try {
				String[] thisQName = IdUtil.parseMultiPartSQLIdentifier(r);
				if (thisQName.length != 2)
					throw StandardException.newException(SQLState.DB_CLASS_PATH_PARSE_ERROR,input);

				v.add(thisQName);
				int delim = r.read();
				if (delim != ':')
				{
					if (delim!=-1)
						throw StandardException.newException(SQLState.DB_CLASS_PATH_PARSE_ERROR,input);
					break;
				}
			}
			
			catch (StandardException se){
			    if (se.getMessageId().equals(SQLState.ID_PARSE_ERROR))
					throw StandardException.newException(SQLState.DB_CLASS_PATH_PARSE_ERROR,
														 se,input);
				else
					throw se;
			}
			
			catch (IOException ioe){
				throw StandardException.newException(SQLState.DB_CLASS_PATH_PARSE_ERROR,ioe,input);
			}
		}
		String[][] result = new String[v.size()][];
		v.copyInto(result);
		return result;
	}


	/*
	** Methods that operate on lists of identifiers.
	*/


	/**
	  Scan a list of comma separated SQL identifiers from the string provided.
      This returns an array with containing the normalized forms of the identifiers.
      
      This raises an an exception if
	  the string does not contain a valid list of names.

	  @exception StandardException Oops
	  */
	public static String[] parseIdList(String p)
		 throws StandardException
	{
		if (p==null) return null;
		StringReader r = new StringReader(p);
		String[] result = parseIdList(r, true);
		verifyEmpty(r);
		return result;
	}
	
	
	/**
	  Parse a list of comma separated SQL identifiers returning
      them a as elements in an array.

	  @param normalize true means return ids in nomral form, false means
	        return them as they were entered.

	  @exception StandardException Oops
	  */
	private static String[] parseIdList(StringReader r, boolean normalize)
		 throws StandardException
	{
		Vector<String> v = new Vector<String>();
		while (true)
		{
			int delim;
			try {
				String thisId = IdUtil.parseId(r,normalize);
				v.add(thisId);
				r.mark(0);
				delim = r.read();
				if (delim != ',')
				{
					if (delim!=-1) r.reset();
					break;
				}
			}
			
			catch (StandardException se){
				if (se.getMessageId().equals(SQLState.ID_LIST_PARSE_ERROR))
					throw StandardException.newException(SQLState.ID_LIST_PARSE_ERROR,se);
				else
					throw se;
			}
			
			catch (IOException ioe){
				throw StandardException.newException(SQLState.ID_LIST_PARSE_ERROR,ioe);
			}
		}
		if (v.size() == 0) return null;
		String[] result = new String[v.size()];
		v.copyInto(result);
		return result;
	}

	/**
	  Return an IdList with all the ids that in l1 and l2
	  or null if not ids are on both lists.

	  @param l1 An array of ids in normal form
	  @param l2 An array of ids in nomral form
	  */
	public static String intersect(String[] l1, String[] l2)
	{
		if (l1 == null || l2 == null) return null;
		HashSet<String> h = new HashSet<String>();
		for(int ix=0;ix<l2.length;ix++) h.add(l2[ix]); 
		Vector<String> v = new Vector<String>();
		for(int ix=0;ix<l1.length;ix++) if (h.contains(l1[ix])) v.add(l1[ix]);
		return vectorToIdList(v,true); 
	}

	/**
	  Return an idList in external form with one id for every 
	  element of v. If v has no elements, return null.

	  @param normal True means the ids in v are in normal form
	         and false means they are in external form.
	  */
	private static String vectorToIdList(Vector<String> v,boolean normal)
	{
		if (v.size() == 0) return null;
		String[] a = new String[v.size()];
		v.copyInto(a);
		if (normal)
			return mkIdList(a);
		else
			return mkIdListAsEntered(a);
	}

	/**
	 * Map userName to authorizationId in its normal form.
	 * 
	 * @exception StandardException on error or userName is null
	 */
	public static String getUserAuthorizationId(String userName) throws StandardException
	{
		try {
            if (userName != null)
			    return parseSQLIdentifier(userName);
		}
		catch (StandardException se) {
		}
        throw StandardException.newException(SQLState.AUTH_INVALID_USER_NAME, userName);
	}

	/**
	 * Get user name from URL properties (key user) without any transformation.
     * If the user property does not exist or is set to the empty string
     * then Property.DEFAULT_USER_NAME is returned.
     * 
     * @see Property#DEFAULT_USER_NAME
	 */
	public static String getUserNameFromURLProps(Properties params)
	{
		String userName = params.getProperty(Attribute.USERNAME_ATTR,
							Property.DEFAULT_USER_NAME);
		if (userName.equals(""))
			userName = Property.DEFAULT_USER_NAME;

		return userName;
	}

	/**
	  Return an IdList with all the ids that are repeated
	  in l.

	  @param l a list of ids in normal form.
	  */
	public static String dups(String[] l)
	{
		if (l == null) return null;
		HashSet<String> h = new HashSet<String>();
		Vector<String> v = new Vector<String>();
		for(int ix=0;ix<l.length;ix++)
		{
			if (!h.contains(l[ix]))
				h.add(l[ix]);
			else
				v.add(l[ix]);
		}
		return vectorToIdList(v,true);
	}
	
	/**
	  Return an IdList with all the duplicate ids removed
	  @param l a list of ids in external form.
	  @exception StandardException Oops.
	  */
	public static String pruneDups(String l) throws StandardException
	{
		if (l == null) return null;
		String[] normal_a = parseIdList(l);
		StringReader r = new StringReader(l);
		String[] external_a = parseIdList(r,false);
		HashSet<String> h = new HashSet<String>();
		Vector<String> v = new Vector<String>();
		for(int ix=0;ix<normal_a.length;ix++)
		{
			if (!h.contains(normal_a[ix]))
			{
				h.add(normal_a[ix]);
				v.add(external_a[ix]);
			}
		}
		return vectorToIdList(v,false);
	}

	/**
	  Produce a string form of an idList from an array of
	  normalized ids.
	  */
	public static String mkIdList(String[] ids)
	{
		StringBuffer sb = new StringBuffer();
		for (int ix=0;ix<ids.length; ix++)
		{
			if (ix != 0) sb.append(",");
			sb.append(IdUtil.normalToDelimited(ids[ix]));
		}
		return sb.toString();
	}

	/**
	  Produce an id list from an array of ids in external form
	  */
	private static String mkIdListAsEntered(String[] externalIds )
	{
		StringBuffer sb = new StringBuffer();
		for (int ix=0;ix<externalIds.length; ix++)
		{
			if (ix != 0) sb.append(",");
			sb.append(externalIds[ix]);
		}
		return sb.toString();
	}

	/**
	  Return true if the normalized value of an indentifier is on the list 
      of SQL identifiers provided.
	  @param id an id in normal form
	  @param	list a list of ids in external form.
	  @exception StandardException oops.
	  */
	public static boolean idOnList(String id, String list)
		 throws StandardException
	{
		if (list==null) return false;
		String[] list_a = parseIdList(list);
		for (int ix=0; ix < list_a.length; ix++)
			if (id.equals(list_a[ix])) return true;
		return false;
	}

	/**
	  Delete an normal value from a list of SQL identifiers.
      The returned list maintains its remaining identifiers in the
      format they were upon entry to the call.
      
      
	  @param id an id in normal form (quotes removed, upshifted if regular)
	  @param list a comma separated list of ids in external
	         form (possibly delmited or not upshifted).
	  @return the list with the id deleted or null if the
	    resulting list has no ids. If 'id' is not on 'list'
		this returns list unchanged. If list becomes empty after the removal
        null is returned.
				 
	  @exception StandardException oops.
	  */
	public static String deleteId(String id, String list)
		 throws StandardException
	{
		if (list==null) return null;
		Vector<String> v = new Vector<String>();
		StringReader r = new StringReader(list);
		String[] enteredList_a = parseIdList(r,false);
        
		//
		//Loop through enteredList element by element
		//removing elements that match id. Before we
		//compare we parse each SQL indentifier in list to convert
		//to normal form.
		for (int ix=0; ix < enteredList_a.length; ix++)
			if (!id.equals(IdUtil.parseSQLIdentifier(enteredList_a[ix])))
				v.add(enteredList_a[ix]);
		if (v.size() == 0)
			return null;
		else
			return vectorToIdList(v,false);
	}


	/**
     * Append an identifier to a comma separated list
     * of identifiers. The passed in identifier is its
     * normal form, the list contains a list of SQL identifiers,
     * either regular or delimited. This routine takes the easy
     * way out and always appends a delimited identifier.
	  @return the list with the id appended in its delimited form. 
	  @exception StandardException oops
	  */
	public static String appendNormalToList(String id, String list)
		 throws StandardException
	{
        String delimitedId = normalToDelimited(id);
		if (list==null)
			return delimitedId;
		else
			return list+","+delimitedId;
	}

	/**
	 * Parse role identifier to internal, case normal form. It should not be
	 * NONE nor exceed Limits.MAX_IDENTIFIER_LENGTH.
	 *
	 * @param roleName role identifier to check (SQL form, has possible quoting)
	 * @return the role name to use (internal, case normal form).
	 * @exception StandardException normal error policy
	 */
	public static String parseRoleId(String roleName) throws StandardException
	{
		roleName = roleName.trim();
		// NONE is a special case and is not allowed with its special
		// meaning in SET ROLE <value specification>. Even if there is
		// a role with case normal form "NONE", we require it to be
		// delimited here, since it would have had to be delimited to
		// get created, too. We could have chosen to be lenient here,
		// but it seems safer to be restrictive.
		if (StringUtil.SQLToUpperCase(roleName).equals("NONE")) {
			throw StandardException.newException(SQLState.ID_PARSE_ERROR);
		}

		roleName = parseSQLIdentifier(roleName);
		checkIdentifierLengthLimit(roleName, Limits.MAX_IDENTIFIER_LENGTH);

		return roleName;
	}

	/**
	 * Check that identifier is not too long
	 * @param identifier identifier (in case normal form) to check
	 * @param identifier_length_limit maximum legal length
	 * @exception StandardException normal error policy
	 */
	public static void checkIdentifierLengthLimit(String identifier,
												  int identifier_length_limit)
			throws StandardException
	{
		if (identifier.length() > identifier_length_limit)
			throw StandardException.newException
				(SQLState.LANG_IDENTIFIER_TOO_LONG,
				 identifier,
				 String.valueOf(identifier_length_limit));
    }
}

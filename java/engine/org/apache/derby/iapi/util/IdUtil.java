/*

   Derby - Class com.ihost.cs.IdUtil

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.util;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Vector;
import java.util.HashSet;
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
	  Delimit the identifier provided.
	  @return the delimited identifier.
	  */
	public static String delimitId(String id)
	{
		StringBuffer quotedBuffer = new StringBuffer();
		quotedBuffer.append('\"');
	    char[] charArray = id.toCharArray();

		for (int ix = 0; ix < charArray.length; ix++){
			char currentChar = charArray[ix];
			quotedBuffer.append(currentChar);
			if (currentChar == '\"')
				quotedBuffer.append('\"');
		}
		quotedBuffer.append('\"');
		
		return quotedBuffer.toString();
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
            return delimitId(id2);
		return
			delimitId(id1) +
			"." +
			delimitId(id2);
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
			sb.append(delimitId(ids[ix]));
		}
		return sb.toString();
	}

	/**
	  Scan a qualified name from the String provided. Raise an excepion
	  if the string does not contain a qualified name.
      
      @param s The string to be parsed
      @param normalizeToUpper If true then undelimited names are converted to upper case (the ANSI standard). If false then undelimited names are converted to lower case (used when the source database is Informix Foundation).
      @return An array of strings made by breaking the input string at its dots, '.'.
	  @exception StandardException Oops
	  */
	public static String[] parseQualifiedName(String s, boolean normalizeToUpper)
		 throws StandardException
	{
		StringReader r = new StringReader(s);
		String[] qName = parseQualifiedName(r, normalizeToUpper);
		verifyEmpty(r);
		return qName;
	}

	/**
	  Scan a qualified name from a StringReader. Return an array
	  of Strings with 1 entry per name scanned. Raise an exception
	  if the StringReader does not contain a valid qualified name.

      @param r A StringReader for the string to be parsed
      @param normalizeToUpper If true then undelimited names are converted to upper case (the ANSI standard). If false then undelimited names are converted to lower case (used when the source database is Informix Foundation).
      @return An array of strings made by breaking the input string at its dots, '.'.
	  @exception StandardException Oops
	  */
	public static String[] parseQualifiedName(StringReader r, boolean normalizeToUpper)
		 throws StandardException
	{
		Vector v = new Vector();
		while (true)
		{
			String thisId = parseId(r,true, normalizeToUpper);
			v.addElement(thisId);
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
	  Convert the String provided to an ID. Throw an exception
	  iff the string does not contain only a valid external form
	  for an id. This is a convenience routine that simply
	  uses getId(StringReader) to do the work.
	  
	  <P> See the header for getId below for restrictions.
	  
	  @exception StandardException Oops
	  */
	public static String parseId(String s)
		 throws StandardException
	{
		StringReader r = new StringReader(s);
		String id = parseId(r,true, true);
		verifyEmpty(r);
		return id;
	}

	/**
	  Read an id from the StringReader provided.


	  @parm nomrlaize true means return ids in nomral form, false means
	        return them as they were entered.

	  <P>
	  Raise an exception if the first thing in the StringReader
	  is not a valid id.

	  @exception StandardException Ooops.
	  */
	public static String parseId(StringReader r, boolean normalize, boolean normalizeToUpper)
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
				return parseUnQId(r,normalize, normalizeToUpper);
		}

		catch (IOException ioe){
			throw StandardException.newException(SQLState.ID_PARSE_ERROR,ioe);
		}
	}

	private static String parseUnQId(StringReader r, boolean normalize, boolean normalizeToUpper)
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

		if (normalize)
			return normalizeToUpper ? StringUtil.SQLToUpperCase(b.toString()) : StringUtil.SQLToLowerCase(b.toString());
		else
			return b.toString();
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
			return delimitId(b.toString()); //Put the quotes back.
	}

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
	public static String[][] parseDbClassPath(String input, boolean normalizeToUpper)
		 throws StandardException
	{
		//As a special case we accept a zero length dbclasspath.
		if (input.length() == 0)
			return new String[0][];

		Vector v = new Vector();
		java.io.StringReader r = new java.io.StringReader(input);
		//
		while (true)
		{
			try {
				String[] thisQName = IdUtil.parseQualifiedName(r, normalizeToUpper);
				if (thisQName.length != 2)
					throw StandardException.newException(SQLState.DB_CLASS_PATH_PARSE_ERROR,input);

				v.addElement(thisQName); 
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
	  Scan a list of ids from the string provided. This returns
	  an array with id per entry. This raises an an exception if
	  the string does not contain a valid list of names.

	  @exception StandardException Oops
	  */
	public static String[] parseIdList(String p)
		 throws StandardException
	{
		if (p==null) return null;
		StringReader r = new StringReader(p);
		String[] result = parseIdList(r, true);
		verifyListEmpty(r);
		return result;
	}
	
	
	/**
	  Parse an idList. 

	  @parm nomralize true means return ids in nomral form, false means
	        return them as they were entered.

	  @exception StandardException Oops
	  */
	private static String[] parseIdList(StringReader r, boolean normalize)
		 throws StandardException
	{
		Vector v = new Vector();
		while (true)
		{
			int delim;
			try {
				String thisId = IdUtil.parseId(r,normalize, true);
				v.addElement(thisId);
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
		HashSet h = new HashSet();
		for(int ix=0;ix<l2.length;ix++) h.add(l2[ix]); 
		Vector v = new Vector();
		for(int ix=0;ix<l1.length;ix++) if (h.contains(l1[ix])) v.addElement(l1[ix]);
		return vectorToIdList(v,true); 
	}

	/**
	  Return an idList in external form with one id for every 
	  element of v. If v has no elements, return null.

	  @param normal True means the ids in v are in normal form
	         and false means they are in external form.
	  */
	private static String vectorToIdList(Vector v,boolean normal)
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
	  Return an IdList with all the ids that are repeated
	  in l.

	  @param l a list of ids in normal form.
	  */
	public static String dups(String[] l)
	{
		if (l == null) return null;
		HashSet h = new HashSet();
		Vector v = new Vector();
		for(int ix=0;ix<l.length;ix++)
		{
			if (!h.contains(l[ix]))
				h.add(l[ix]);
			else
				v.addElement(l[ix]);
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
		HashSet h = new HashSet();
		Vector v = new Vector();
		for(int ix=0;ix<normal_a.length;ix++)
		{
			if (!h.contains(normal_a[ix]))
			{
				h.add(normal_a[ix]);
				v.addElement(external_a[ix]);
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
			sb.append(IdUtil.delimitId(ids[ix]));
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

	private static void verifyListEmpty(StringReader r)
		 throws StandardException
	{
		try {
			if (r.read() != -1)
				throw StandardException.newException(SQLState.ID_LIST_PARSE_ERROR);
		}

		catch (IOException ioe){
			throw StandardException.newException(SQLState.ID_LIST_PARSE_ERROR,ioe);
		}
		

	}

	/**
	  Return true if the id provided is on the list provided.
	  @param id an id in normal form
	  @list a list of ids in external form.
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
	  Delete an id from a list of ids.
	  @param id an id in normal form (quotes removed, upshifted)
	  @param list a comma separated list of ids in external
	         form (possibly delmited or not upshifted).
	  @return the list with the id deleted or null if the
	    resulting list has no ids. If 'id' is not on 'list'
		this returns list unchanged.
				 
	  @exception StandardException oops.
	  */
	public static String deleteId(String id, String list)
		 throws StandardException
	{
		if (list==null) return null;
		Vector v = new Vector();
		StringReader r = new StringReader(list);
		String[] enteredList_a = parseIdList(r,false);
		//
		//Loop through enteredList element by element
		//removing elements that match id. Before we
		//compare we parse each id in list to convert
		//to normal form.
		for (int ix=0; ix < enteredList_a.length; ix++)
			if (!id.equals(IdUtil.parseId(enteredList_a[ix])))
				v.addElement(enteredList_a[ix]);
		if (v.size() == 0)
			return null;
		else
			return vectorToIdList(v,false);
	}


	/**
	  Append an id in external form.
	  @return the list with the id appended. 
	  @exception StandardException oops
	  */
	public static String appendId(String id, String list)
		 throws StandardException
	{
		if (list==null)
			return id;
		else
			return list+","+id;
	}
}

/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.FileResource;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.StringBuffer;
import java.net.URL;

/**
  Utility class for testing files stored in the database.
  */ 
public class DbFile
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	  Read the current generation of a file stored in the
	  database we are connected to and return a 1 line string
	  representation of the file.

	  Sample usage
	  values org.apache.derbyTesting.functionTests.util.DbFile::readAsString('S1','J1');
	  @exception Exception Oops.
	  */
/*	
CANT USE JarAccess - not a public API (actually it's gone!)
public static String
	readAsString(String schemaName, String sqlName)
		 throws Exception
	{
		InputStream is = JarAccess.getAsStream(schemaName,
											sqlName,
 											FileResource.CURRENT_GENERATION_ID);
		return stringFromFile(is);
	}
*/
	/**
	  Create a string that contains a representation of the content of
	  a file for testing.
	  @exception Exception Oops.
	  */
	public static String
	stringFromFile(InputStream is)
		 throws Exception
	{
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br =
			new BufferedReader(isr);
		StringBuffer sb = new StringBuffer();
		String l;
		while((l = br.readLine()) != null) {
			sb.append(l);
			sb.append("<CR>");
		}
		is.close();
		return sb.toString();
	}

	/**
	  Get the URL for a resource.

	  @param packageName the name of the resource package
	  @param name the name of the resourse.
	  */
	public static URL
	getResourceURL(String packageName, String name)
	{
		String resourceName =
			"/"+
			packageName.replace('.','/')+
			"/"+
			name;
		//
		//We need a class to get our URL. Since we give a
		//fully qualified name for the URL, any class will
		//do.
		Class c = resourceName.getClass();
		URL url = c.getResource(resourceName);
		return url;
	}

	/**
	  Get an InputStream for reading a resource.

	  @param packageName the name of the resource package
	  @param name the name of the resourse.
	  @exception Exception Oops.
	  */
	public static InputStream
	getResourceAsStream(String packageName, String name)
	{
		String resourceName =
			"/"+
			packageName.replace('.','/')+
			"/"+
			name;
		//
		//We need a class to get our URL. Since we give a
		//fully qualified name for the URL, any class will
		//do.
		Class c = resourceName.getClass();
		InputStream result = c.getResourceAsStream(resourceName);
		return result;
	}

	public	static	boolean	deleteFile( String outputFileName )
		 throws Exception
	{
		File f = new File( outputFileName );

		return f.delete();
	}

	public static String mkFileFromResource
	(String packageName, String resourceName)
		 throws Exception
	{
		return mkFileFromResource( packageName, resourceName, resourceName );
	}

	public static String mkFileFromResource
	( String packageName, String resourceName, String outputFileName )
		 throws Exception
	{
		File f = new File( outputFileName );
		InputStream is = getResourceAsStream(packageName,resourceName);
		BufferedInputStream bis = new BufferedInputStream(is);
		OutputStream os = new FileOutputStream(f);
		byte[]buf=new byte[4096];
		int readThisTime = 0;
		while((readThisTime = bis.read(buf)) != -1)
			os.write(buf,0,readThisTime);
		os.close();
		return f.getAbsolutePath();
	}
}
 

/*

   Derby - Class org.apache.derbyTesting.functionTests.util.FTFileUtil

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

package org.apache.derbyTesting.functionTests.util;

import java.io.FileWriter;
import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
  Convience functions for performing file manipulations
  in ij scripts.
  */
public class FTFileUtil
{ 
	/**
	  Create a file.

	  @param fileName the file's name.
	  @param length the number of bytes of test data in the file.
	  @exception Exception oops.
	  */
	public static void mkFile(String fileName, int length) throws Exception
	{
		FileWriter fw = new FileWriter(fileName);
		int offset = 0;
		String data = "Amber!";
		for (int ix=0;ix<length;ix++)
		{
			fw.write(data,offset,1);
			offset++;
			if (offset >= data.length()) offset = 0;
		}
		fw.close();
	}

	/**
     * rename a file. 
     * This method is  called by some tests through a SQL procedure:
     * RENAME_FILE(LOCATION VARCHAR(32000), NAME VARCHAR(32000), 
     *                                 NEW_NAME  VARCHAR(32000))
     * @param location location of the file
     * @param name the file's name
	 * @param newName the file's new name
	*/
	public static void renameFile(String location, String name , 
                                  String newName) throws Exception
	{
		final File src = new File(location, name);
		final File dst = new File(location, newName);
        
        // needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it.
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    if(!src.renameTo(dst))
                    {
                        throw new Exception("unable to rename File: " +
                                            src.getAbsolutePath() +
                                            " To: " + dst.getAbsolutePath());
                    }
                    
                    return null; // nothing to return
                }
            });
    }


    /**
     * Check if a file exists ?
     *
     * This method is  called by some tests through a SQL function:
     * fileExists(fileName varchar(128))returns VARCHAR(100)
     *
     * @param fileName the file's name.
     * @return     <tt>"true"</tt> if the given file exists 
     *             <tt>"false"</tt> otherwise.
     */
    public static String fileExists(String fileName) 
    {
        final File fl = new File(fileName);
                
        // needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it.

        return Boolean.toString(PrivilegedFileOpsForTests.exists(fl));
    }


    /**
     *	Remove a directory and all of its contents.
     *   
     *  @param directory the directory's name.
     * @return     <tt>true</tt> if the omplete directory was removed
     *             <tt>false</tt> otherwise.f false is returned then some of 
     *              the files in the directory may have been removed.
     */

	private static boolean removeDirectory(File directory) {

		if (directory == null)
			return false;
		if (!directory.exists())
			return true;
		if (!directory.isDirectory())
			return false;

		String[] list = directory.list();

		if (list != null) {
			for (int i = 0; i < list.length; i++) {
				File entry = new File(directory, list[i]);

				if (entry.isDirectory())
				{
					if (!removeDirectory(entry))
						return false;
				}
				else
				{
					if (!entry.delete())
						return false;
				}
			}
		}

		return directory.delete();
	}

    /**
     * Remove a directory and all of its contents.
     * This method is  called by some tests through a SQL function:
     * removeDirectory(fileName varchar(128)) returns VARCHAR(100)
     *   
     * @param directory the directory's name.
     * @return     <tt>"true"</tt> if the omplete directory was removed
     *             <tt>"false"</tt> otherwise.f false is returned then some of 
     *              the files in the directory may have been removed.
     */

	public static String removeDirectory(final String directory)
        throws PrivilegedActionException
	{
        // needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it.

        return AccessController.doPrivileged(new PrivilegedAction<String>() {
                    public String run()
                    {
                        return (removeDirectory(
                               new File(directory)) ? "true" : "false");
                    }
                });
	}
    
}

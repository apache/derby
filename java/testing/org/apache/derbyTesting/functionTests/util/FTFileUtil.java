/*

   Derby - Class org.apache.derbyTesting.functionTests.util.FTFileUtil

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;

import java.io.FileWriter;
import java.io.File;
/**
  Convience functions for performing file manipulations
  in ij scripts.
  */
public class FTFileUtil
{ 
	/**
	  Create a file.

	  @param name the file's name.
	  @length the number of bytes of test data in the file.
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
	   rename a file
	   @param location location of the file
	   @param name the file's name
	   @param newName the file's new name
	*/
	public static void renameFile(String location, String name , String newName) throws Exception
	{
		File src = new File(location, name);
		File dst = new File(location, newName);
		if(!src.renameTo(dst))
		{
			throw new Exception("unable to rename File: " +
								src.getAbsolutePath() +
							    " To: " + dst.getAbsolutePath());
		}
	}
}




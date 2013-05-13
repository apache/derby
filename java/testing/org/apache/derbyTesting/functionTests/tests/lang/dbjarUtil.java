/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.cursor

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
	Simple program to archive a database up in a jar file
	within the test harness.
*/

public class dbjarUtil
{
	/**
		jarname - jarname to use
		path - path to database
		dbname - database name in archive
	*/
	public static void createArchive(String jarName, String path, String dbName) throws Exception {

		String root = System.getProperty("derby.system.home", System.getProperty("user.dir"));

		// get list of files
		File top = new File(root, path);

		if (!top.isDirectory())
			throw new Exception(top.toString() + " is not a directory");

		// jar file paths in the JDB CURL are relative to the root
		// derby.system.home or user.dir, so need to create the jar there.
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(new File(root, jarName))); 

		addEntries(zos, top, dbName, top.getPath().length());
		
		zos.close(); 
	}


	static void addEntries(ZipOutputStream zos, File dir, String dbName, int old) throws Exception {

		String[] list = dir.list();

		for (int i = 0; i < list.length; i++) {

			File f = new File(dir, list[i]);
			if (f.isDirectory()) {
				addEntries(zos, f, dbName, old);
			} else {
				addFile(zos, f, dbName, old);
			}

		}
	}




    static void addFile(
        ZipOutputStream zos, 
        File f, String dbName, int old) throws IOException
    {

		String s = f.getPath().replace(File.separatorChar, '/');

		s = s.substring(old);

		s = dbName.concat(s);

		// jar has forward slashes!
        ZipEntry ze= new ZipEntry(s); 
        ze.setTime(f.lastModified()); 

        zos.putNextEntry(ze); 

		byte[] byte8= new byte[1024]; 
        BufferedInputStream bufferedInputStream10= new BufferedInputStream((new FileInputStream(f))); 
        while (true)
        {
            int int9= bufferedInputStream10.read(byte8, 0, byte8.length); 
            if (int9 == -1)
            {
                break;
            }
            zos.write(byte8, 0, int9); 
        }

        bufferedInputStream10.close(); 
        zos.closeEntry(); 
    }
  
    public static void setDBContextClassLoader(String jarName) throws MalformedURLException
    {
		String root = System.getProperty("derby.system.home", System.getProperty("user.dir"));

		File jar = new File(root, jarName);
		
		URLClassLoader cl = new URLClassLoader(new URL[] {jar.toURI().toURL()});
    	java.lang.Thread.currentThread().setContextClassLoader(cl);
   
    }

    public static void setNullContextClassLoader()
    {
    	java.lang.Thread.currentThread().setContextClassLoader(null);
    }

}


/*

   Derby - Class org.apache.derby.iapi.services.io.FileUtil

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;

import java.io.*;
import java.net.*;

/**
	A set of public static methods for dealing with File objects.
*/
public abstract class FileUtil {

    private static final int BUFFER_SIZE = 4096*4;
	/**
		Remove a directory and all of its contents.

		The results of executing File.delete() on a File object
		that represents a directory seems to be platform
		dependent. This method removes the directory
		and all of its contents.

		@return true if the complete directory was removed, false if it could not be.
		If false is returned then some of the files in the directory may have been removed.

	*/
	public static boolean removeDirectory(File directory) {

		// System.out.println("removeDirectory " + directory);

		if (directory == null)
			return false;
		if (!directory.exists())
			return true;
		if (!directory.isDirectory())
			return false;

		String[] list = directory.list();

		// Some JVMs return null for File.list() when the
		// directory is empty.
		if (list != null) {
			for (int i = 0; i < list.length; i++) {
				File entry = new File(directory, list[i]);

				//				System.out.println("\tremoving entry " + entry);

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

	public static boolean removeDirectory(String directory)
	{
	    return removeDirectory(new File(directory));
	}

	/**
	  Copy a directory and all of its contents.
	  */
	public static boolean copyDirectory(File from, File to)
	{
		return copyDirectory(from, to, (byte[])null, (String[])null);
	}

	public static boolean copyDirectory(String from, String to)
	{
		return copyDirectory(new File(from), new File(to));
	}

	/**
		@param filter - array of names to not copy.
	*/
	public static boolean copyDirectory(File from, File to, byte[] buffer, 
										String[] filter)
	{
		//
		// System.out.println("copyDirectory("+from+","+to+")");		

		if (from == null)
			return false;
		if (!from.exists())
			return true;
		if (!from.isDirectory())
			return false;

		if (to.exists())
		{
			//			System.out.println(to + " exists");
			return false;
		}
		if (!to.mkdirs())
		{
			//			System.out.println("can't make" + to);
			return false;
		}			

		String[] list = from.list();

		// Some JVMs return null for File.list() when the
		// directory is empty.
		if (list != null) {

			if (buffer == null)
				buffer = new byte[BUFFER_SIZE]; // reuse this buffer to copy files

nextFile:	for (int i = 0; i < list.length; i++) {

				String fileName = list[i];

				if (filter != null) {
					for (int j = 0; j < filter.length; j++) {
						if (fileName.equals(filter[j]))
							continue nextFile;
					}
				}


				File entry = new File(from, fileName);

				//				System.out.println("\tcopying entry " + entry);

				if (entry.isDirectory())
				{
					if (!copyDirectory(entry,new File(to,fileName),buffer,filter))
						return false;
				}
				else
				{
					if (!copyFile(entry,new File(to,fileName),buffer))
						return false;
				}
			}
		}
		return true;
	}		

	public static boolean copyFile(File from, File to)
	{
		return copyFile(from, to, (byte[])null);
	}

	public static boolean copyFile(File from, File to, byte[] buf)
	{
		if (buf == null)
			buf = new byte[BUFFER_SIZE];

		//
		//		System.out.println("Copy file ("+from+","+to+")");
		FileInputStream from_s = null;
		FileOutputStream to_s = null;

		try {
			from_s = new FileInputStream(from);
			to_s = new FileOutputStream(to);

			for (int bytesRead = from_s.read(buf);
				 bytesRead != -1;
				 bytesRead = from_s.read(buf))
				to_s.write(buf,0,bytesRead);

			from_s.close();
			from_s = null;

			to_s.getFD().sync();  // RESOLVE: sync or no sync?
			to_s.close();
			to_s = null;
		}
		catch (IOException ioe)
		{
			return false;
		}
		finally
		{
			if (from_s != null)
			{
				try { from_s.close(); }
				catch (IOException ioe) {}
			}
			if (to_s != null)
			{
				try { to_s.close(); }
				catch (IOException ioe) {}
			}
		}

		return true;
	}

    public static boolean copyDirectory( StorageFactory storageFactory,
                                         StorageFile from,
                                         File to)
    {
        return copyDirectory( storageFactory, from, to, null, null);
    }
    
    public static boolean copyDirectory( StorageFactory storageFactory,
                                         StorageFile from,
                                         File to,
                                         byte[] buffer,
                                         String[] filter)
    {
		if (from == null)
			return false;
		if (!from.exists())
			return true;
		if (!from.isDirectory())
			return false;

		if (to.exists())
		{
			//			System.out.println(to + " exists");
			return false;
		}
		if (!to.mkdirs())
		{
			//			System.out.println("can't make" + to);
			return false;
		}			

		String[] list = from.list();

		// Some JVMs return null for File.list() when the
		// directory is empty.
		if (list != null)
        {
			if (buffer == null)
				buffer = new byte[BUFFER_SIZE]; // reuse this buffer to copy files

          nextFile:
            for (int i = 0; i < list.length; i++)
            {
				String fileName = list[i];

				if (filter != null) {
					for (int j = 0; j < filter.length; j++) {
						if (fileName.equals(filter[j]))
							continue nextFile;
					}
				}

				StorageFile entry = storageFactory.newStorageFile(from, fileName);

				if (entry.isDirectory())
				{
					if (!copyDirectory( storageFactory, entry, new File(to,fileName), buffer, filter))
						return false;
				}
				else
				{
					if (!copyFile( storageFactory, entry, new File(to,fileName), buffer))
						return false;
				}
			}
		}
		return true;
	} // end of copyDirectory( StorageFactory sf, StorageFile from, File to, byte[] buf, String[] filter)

    public static boolean copyFile( StorageFactory storageFactory, StorageFile from, File to)
    {
        return copyFile( storageFactory, from, to, (byte[]) null);
    }
    
	public static boolean copyFile( StorageFactory storageFactory, StorageFile from, File to, byte[] buf)
	{
		InputStream from_s = null;
		FileOutputStream to_s = null;

		try {
			from_s = from.getInputStream();
			to_s = new FileOutputStream( to);

			if (buf == null)
				buf = new byte[BUFFER_SIZE]; // reuse this buffer to copy files

			for (int bytesRead = from_s.read(buf);
				 bytesRead != -1;
				 bytesRead = from_s.read(buf))
				to_s.write(buf,0,bytesRead);

			from_s.close();
			from_s = null;

			to_s.getFD().sync();  // RESOLVE: sync or no sync?
			to_s.close();
			to_s = null;
		}
		catch (IOException ioe)
		{
			return false;
		}
		finally
		{
			if (from_s != null)
			{
				try { from_s.close(); }
				catch (IOException ioe) {}
			}
			if (to_s != null)
			{
				try { to_s.close(); }
				catch (IOException ioe) {}
			}
		}

		return true;
	} // end of copyFile( StorageFactory storageFactory, StorageFile from, File to, byte[] buf)

    public static boolean copyDirectory( WritableStorageFactory storageFactory,
                                         File from,
                                         StorageFile to)
    {
        return copyDirectory( storageFactory, from, to, null, null);
    }
    
    public static boolean copyDirectory( WritableStorageFactory storageFactory,
                                         File from,
                                         StorageFile to,
                                         byte[] buffer,
                                         String[] filter)
    {
		if (from == null)
			return false;
		if (!from.exists())
			return true;
		if (!from.isDirectory())
			return false;

		if (to.exists())
		{
			//			System.out.println(to + " exists");
			return false;
		}
		if (!to.mkdirs())
		{
			//			System.out.println("can't make" + to);
			return false;
		}			

		String[] list = from.list();

		// Some JVMs return null for File.list() when the
		// directory is empty.
		if (list != null)
        {
			if (buffer == null)
				buffer = new byte[BUFFER_SIZE]; // reuse this buffer to copy files

          nextFile:
            for (int i = 0; i < list.length; i++)
            {
				String fileName = list[i];

				if (filter != null) {
					for (int j = 0; j < filter.length; j++) {
						if (fileName.equals(filter[j]))
							continue nextFile;
					}
				}

				File entry = new File(from, fileName);

				if (entry.isDirectory())
				{
					if (!copyDirectory( storageFactory, entry, storageFactory.newStorageFile(to,fileName), buffer, filter))
						return false;
				}
				else
				{
					if (!copyFile( storageFactory, entry, storageFactory.newStorageFile(to,fileName), buffer))
						return false;
				}
			}
		}
		return true;
	} // end of copyDirectory( StorageFactory sf, StorageFile from, File to, byte[] buf, String[] filter)

    public static boolean copyFile( WritableStorageFactory storageFactory, File from, StorageFile to)
    {
        return copyFile( storageFactory, from, to, (byte[]) null);
    }
    
	public static boolean copyFile( WritableStorageFactory storageFactory, File from, StorageFile to, byte[] buf)
	{
		InputStream from_s = null;
		OutputStream to_s = null;

		try {
			from_s = new FileInputStream( from);
			to_s = to.getOutputStream();

			if (buf == null)
				buf = new byte[BUFFER_SIZE]; // reuse this buffer to copy files

			for (int bytesRead = from_s.read(buf);
				 bytesRead != -1;
				 bytesRead = from_s.read(buf))
				to_s.write(buf,0,bytesRead);

			from_s.close();
			from_s = null;

			storageFactory.sync( to_s, false);  // RESOLVE: sync or no sync?
			to_s.close();
			to_s = null;
		}
		catch (IOException ioe)
		{
			return false;
		}
		finally
		{
			if (from_s != null)
			{
				try { from_s.close(); }
				catch (IOException ioe) {}
			}
			if (to_s != null)
			{
				try { to_s.close(); }
				catch (IOException ioe) {}
			}
		}

		return true;
	} // end of copyFile

	/**
		Convert a file path into a File object with an absolute path
		relative to a passed in root. If path is absolute then
		a file object constructed from new File(path) is returned,
		otherwise a file object is returned from new File(root, path)
		if root is not null, otherwise null is returned.
	*/
	public static File getAbsoluteFile(File root, String path) {
		File file = new File(path);
		if (file.isAbsolute())
			return file;

		if (root == null)
			return null;

		return new File(root, path);
	}

	/**
		A replacement for new File(File, String) that correctly implements
		the case when the first argument is null. The documentation for java.io.File
		says that new File((File) null, name) is the same as new File(name).
		This is not the case in pre 1.1.8 vms, a NullPointerException is thrown instead.
	*/
	public static File newFile(File parent, String name) {

		if (parent == null)
			return new File(name);
		else
			return new File(parent, name);
	}

	/**
	 * Open an input stream to read a file or a URL
	 * @param fileOrURL	The file or URL to open.
	 * @param bufferSize 0 => no buffering.
	 * @return	an InputStream
	 * @exception StandardException	Thrown on failure
	 */
	public static InputStream getInputStream(String fileOrURL,int bufferSize)
		 throws IOException
	{
		InputStream is;
		try {
			is = new FileInputStream( fileOrURL );
		}

		catch (FileNotFoundException fnfe){
			try {
				is = new URL( fileOrURL ).openStream();
			} catch (MalformedURLException mfurle) {

				// if it looks like an url throw this exception
				// otherwise throw the file not found exception
				// If there is no : or an early colon then it's
				// probably a file (e.g. /foo/myjar.jar or a:/foo/myjar.jar)
				if (fileOrURL.indexOf(':') > 2)
					throw mfurle;
				throw fnfe;
			}
		}
		if (bufferSize > 0)
			is = new BufferedInputStream(is,bufferSize);

		return is;
	}
}

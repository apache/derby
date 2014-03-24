/*

   Derby - Class org.apache.derby.iapi.services.io.FileUtil

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

package org.apache.derby.iapi.services.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;

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
            limitAccessToOwner(to);

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
                                         File to,
                                         byte[] buffer,
                                         String[] filter, 
                                         boolean copySubDirs)
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

        try {
            limitAccessToOwner(to);
        } catch (IOException ioe) {
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
                    if(copySubDirs) {
                        if (!copyDirectory( storageFactory, entry, 
                                            new File(to,fileName), buffer, 
                                            filter, copySubDirs))
                            return false;
                    }
                    else {
                        // the request is to not copy the directories, continue
                        // to the next file in the list.
                        continue nextFile;
                    }

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
            limitAccessToOwner(to);

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

        try {
            to.limitAccessToOwner();
        } catch (IOException ioe) {
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


    public static boolean copyFile( WritableStorageFactory storageFactory, 
                                    StorageFile from, StorageFile to)
    {
		InputStream from_s = null;
		OutputStream to_s = null;

		try {
			from_s = from.getInputStream();
			to_s = to.getOutputStream();

            byte[] buf = new byte[BUFFER_SIZE];

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
		Remove the leading 'file://' protocol from a filename which has been
        expressed as an URL. If the filename is not an URL, then nothing is done.
        Otherwise, an URL like 'file:///tmp/foo.txt' is transformed into the legal
        file name '/tmp/foo.txt'.
	*/
    public static String stripProtocolFromFileName( String originalName )
    {
        String result = originalName;
        try {
            URL url = new URL(originalName);
            result = url.getFile();
        } catch (MalformedURLException ex) {}

        return result;
    }


    // Members used by limitAccessToOwner
    private final static FilePermissionService filePermissionService =
            loadFilePermissionService();

    private static FilePermissionService loadFilePermissionService() {
        try {
            Class cl = Class.forName(
                    FilePermissionService.class.getName() + "Impl");
            return (FilePermissionService) cl.newInstance();
        } catch (ClassNotFoundException ex) {
        } catch (InstantiationException ex) {
        } catch (IllegalAccessException ex) {
        } catch (LinkageError e) {
        }

        // Could not create an instance. This most likely means we are
        // not on Java 7 or higher. Just return null, and let
        // limitAccessToOwner() choose another strategy on older platforms.
        return null;
    }

    /**
     * Use when creating new files. If running on Unix,
     * limit read and write permissions on {@code file} to owner if {@code
     * derby.storage.useDefaultFilePermissions == false}.
     * <p/>
     * If the property is not specified, we use restrictive permissions anyway
     * iff running with the server server started from the command line.
     * <p/>
     * On Unix, this is equivalent to running with umask 0077.
     * <p/>
     * On Windows, with FAT/FAT32, we lose, since the fs does not support
     * permissions, only a read-only flag.
     * <p/>
     * On Windows, with NTFS with ACLs, if running with Java 7 or higher, we
     * limit access also for Windows using the new {@code
     * java.nio.file.attribute} package.
     *
     * @param file assumed to be just created
     * @throws IOException if an I/O error happens when trying to change the
     *   file permissions
     */
    public static void limitAccessToOwner(File file) throws IOException {

        String value = PropertyUtil.getSystemProperty(
            Property.STORAGE_USE_DEFAULT_FILE_PERMISSIONS);

        if (value != null) {
            if (Boolean.valueOf(value.trim()).booleanValue()) {
                return;
            }
        } else {
            // The property has not been specified. Only proceed if we are
            // running with the network server started from the command line
            // *and* at Java 7 or above
            if (JVMInfo.JDK_ID >= JVMInfo.J2SE_17 &&
                    (PropertyUtil.getSystemBoolean(
                        Property.SERVER_STARTED_FROM_CMD_LINE, false)) ) {
                // proceed
            } else {
                return;
            }
        }

        if (limitAccessToOwnerViaACLs(file)) {
            return;
        }

        //
        // First switch off all write access
        //
        assertTrue(file.setWritable(false, false));

        //
        // Next, switch on write again, but for owner only
        //
        assertTrue(file.setWritable(true, true));

        //
        // First switch off all read access
        //
        assertTrue(file.setReadable(false, false));

        //
        // Next, switch on read access again, but for owner only
        //
        assertTrue(file.setReadable(true, true));

        if (file.isDirectory()) {
            //
            // First switch off all exec access
            //
            assertTrue(file.setExecutable(false, false));

            //
            // Next, switch on exec again, but for owner only
            //
            assertTrue(file.setExecutable(true, true));
        }
    }

    private static void assertTrue(boolean b) {
        // We should always have the permission to modify the access since have
        // just created the file. On some file systems, some operations will
        // not work, though, notably FAT/FAT32, as well as NTFS on java < 7, so
        // we ignore it the failure.
        if (SanityManager.DEBUG) {
            if (!b) {
                String os =
                    PropertyUtil.getSystemProperty("os.name").toLowerCase();

                if (os.indexOf("windows") >= 0) {
                    // expect this to fail, Java 6 on Windows doesn't cut it,
                    // known not to work.
                } else {
                    SanityManager.THROWASSERT(
                        "File.set{RWX} failed on this file system");
                }
            }
        }
    }

    private static boolean limitAccessToOwnerViaACLs(File file)
            throws IOException {

        // See if we are running on JDK 7 so we can deny access
        // using the new java.nio.file.attribute package.

        if (filePermissionService == null) {
            // nope
            return false;
        }

        // We have Java 7, so call.
        return filePermissionService.limitAccessToOwner(file);
    }
}

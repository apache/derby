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
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFile;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

        limitAccessToOwner(to);

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

        limitAccessToOwner(to);

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

        to.limitAccessToOwner();

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
        return copyFile( storageFactory, from, to, (byte[]) null);
    }
    
	public static boolean copyFile( WritableStorageFactory storageFactory, 
                                    StorageFile from, StorageFile to, 
                                    byte[] buf)
	{
		InputStream from_s = null;
		OutputStream to_s = null;

		try {
			from_s = from.getInputStream();
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
    private static final Object region = new Object();
    private static boolean initialized = false;

    // Reflection helper objects for calling into Java >= 6
    private static Method setWrite = null;
    private static Method setRead = null;
    private static Method setExec = null;

    // Reflection helper objects for calling into Java >= 7
    private static Class fileClz = File.class;
    private static Class filesClz;
    private static Class pathClz;
    private static Class pathsClz;
    private static Class aclEntryClz;
    private static Class aclFileAttributeViewClz;
    private static Class posixFileAttributeViewClz;
    private static Class userPrincipalClz;
    private static Class linkOptionArrayClz;
    private static Class linkOptionClz;
    private static Class stringArrayClz;
    private static Class aclEntryBuilderClz;
    private static Class aclEntryTypeClz;
    private static Class fileStoreClz;

    private static Method get;
    private static Method getFileAttributeView;
    private static Method supportsFileAttributeView;
    private static Method getFileStore;
    private static Method getOwner;
    private static Method getAcl;
    private static Method setAcl;
    private static Method principal;
    private static Method getName;
    private static Method build;
    private static Method newBuilder;
    private static Method setPrincipal;
    private static Method setType;

    private static Field allow;
    /**
     * Use when creating new files. If running with Java 6 or higher on Unix,
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
     */
    public static void limitAccessToOwner(File file) {

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

        // lazy initialization, needs to be called in security context
        synchronized (region) {
            if (!initialized) {
                initialized = true;
                // >= Java 6
                try {
                    setWrite = fileClz.getMethod(
                        "setWritable",
                        new Class[]{Boolean.TYPE, Boolean.TYPE});
                    setRead = fileClz.getMethod(
                        "setReadable",
                        new Class[]{Boolean.TYPE, Boolean.TYPE});
                    setExec = fileClz.getMethod(
                        "setExecutable",
                        new Class[]{Boolean.TYPE, Boolean.TYPE});
                } catch (NoSuchMethodException e) {
                    // not Java 6 or higher
                }

                // >= Java 7
                try {
                    // If found, we have >= Java 7.
                    filesClz = Class.forName(
                        "java.nio.file.Files");
                    pathClz = Class.forName(
                        "java.nio.file.Path");
                    pathsClz = Class.forName(
                        "java.nio.file.Paths");
                    aclEntryClz = Class.forName(
                        "java.nio.file.attribute.AclEntry");
                    aclFileAttributeViewClz = Class.forName(
                        "java.nio.file.attribute.AclFileAttributeView");
                    posixFileAttributeViewClz = Class.forName(
                        "java.nio.file.attribute.PosixFileAttributeView");
                    userPrincipalClz = Class.forName(
                        "java.nio.file.attribute.UserPrincipal");
                    linkOptionArrayClz = Class.forName(
                        "[Ljava.nio.file.LinkOption;");
                    linkOptionClz = Class.forName(
                        "java.nio.file.LinkOption");
                    stringArrayClz = Class.forName(
                        "[Ljava.lang.String;");
                    aclEntryBuilderClz = Class.forName(
                        "java.nio.file.attribute.AclEntry$Builder");
                    aclEntryTypeClz = Class.forName(
                        "java.nio.file.attribute.AclEntryType");
                    fileStoreClz = Class.forName(
                        "java.nio.file.FileStore");
                    get = pathsClz.getMethod(
                        "get",
                        new Class[]{String.class, stringArrayClz});
                    getFileAttributeView = filesClz.getMethod(
                        "getFileAttributeView",
                        new Class[]{pathClz, Class.class, linkOptionArrayClz});
                    supportsFileAttributeView = fileStoreClz.getMethod(
                        "supportsFileAttributeView",
                        new Class[]{Class.class});
                    getFileStore = filesClz.getMethod("getFileStore",
                                                      new Class[]{pathClz});
                    getOwner = filesClz.
                        getMethod("getOwner",
                                  new Class[]{pathClz, linkOptionArrayClz});
                    getAcl = aclFileAttributeViewClz.
                        getMethod("getAcl", new Class[]{});
                    setAcl = aclFileAttributeViewClz.
                        getMethod("setAcl", new Class[]{List.class});
                    principal = aclEntryClz.
                        getMethod("principal", new Class[]{});
                    getName = userPrincipalClz.
                        getMethod("getName", new Class[]{});
                    build = aclEntryBuilderClz.
                        getMethod("build", new Class[]{});
                    newBuilder = aclEntryClz.
                        getMethod("newBuilder", new Class[]{});
                    setPrincipal = aclEntryBuilderClz.
                        getMethod("setPrincipal",
                                  new Class[]{userPrincipalClz});
                    setType = aclEntryBuilderClz.
                        getMethod("setType", new Class[]{aclEntryTypeClz});

                    allow = aclEntryTypeClz.getField("ALLOW");

                } catch (NoSuchMethodException e) {
                    // not Java 7 or higher
                } catch (ClassNotFoundException e) {
                    // not Java 7 or higher
                } catch (NoSuchFieldException e) {
                    // not Java 7 or higher
                }
            }
        }

        if (setWrite == null) {
            // JVM level too low
            return;
        }

        if (limitAccessToOwnerViaACLs(file)) {
            return;
        }

        try {
            //
            // First switch off all write access
            //
            Object r;

            r = setWrite.invoke(
                file,
                new Object[]{Boolean.FALSE, Boolean.FALSE});
            assertTrue(r);

            //
            // Next, switch on write again, but for owner only
            //
            r = setWrite.invoke(
                file,
                new Object[]{Boolean.TRUE, Boolean.TRUE});
            assertTrue(r);

            //
            // First switch off all read access
            //
            r = setRead.invoke(
                file,
                new Object[]{Boolean.FALSE, Boolean.FALSE});
            assertTrue(r);

            //
            // Next, switch on read access again, but for owner only
            //
            r = setRead.invoke(
                file,
                new Object[]{Boolean.TRUE, Boolean.TRUE});
            assertTrue(r);


            if (file.isDirectory()) {
                //
                // First switch off all exec access
                //
                r = setExec.invoke(
                    file,
                    new Object[]{Boolean.FALSE, Boolean.FALSE});
                assertTrue(r);

                //
                // Next, switch on read exec again, but for owner only
                //
                r = setExec.invoke(
                    file,
                    new Object[]{Boolean.TRUE, Boolean.TRUE});
                assertTrue(r);
            }
        } catch (InvocationTargetException e) {
            // setWritable/setReadable can throw SecurityException
            throw (SecurityException)e.getCause();
        } catch (IllegalAccessException e) {
            // coding error
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(e);
            }
        }
    }

    private static void assertTrue(Object r){
        // We should always have the permission to modify the access since have
        // just created the file. On some file systems, some operations will
        // not work, though, notably FAT/FAT32, as well as NTFS on java < 7, so
        // we ignore it the failure.
        if (SanityManager.DEBUG) {
            Boolean b = (Boolean)r;

            if (!b.booleanValue()) {
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

    private static boolean limitAccessToOwnerViaACLs(File file) {

        // See if we are running on JDK 7 so we can deny access
        // using the new java.nio.file.attribute package.

        if (filesClz == null) {
            // nope
            return false;
        }

        // We have Java 7, so call. We need to call reflectively, since the
        // source level isn't yet at Java 7.
        try {
            // Path fileP = Paths.get(file.getPath());
            Object fileP = get.invoke(
                null, new Object[]{file.getPath(), new String[]{}});

            // ACLs supported on this platform, now check the current file
            // system:
            Object fileStore = getFileStore.invoke(
                null,
                new Object[]{fileP});

            boolean supported =
                ((Boolean)supportsFileAttributeView.invoke(
                    fileStore,
                    new Object[]{aclFileAttributeViewClz})).booleanValue();

            if (!supported) {
                return false;
            }


            // AclFileAttributeView view =
            //     Files.getFileAttributeView(fileP,
            //         AclFileAttributeView.class);
            Object view = getFileAttributeView.invoke(
                null,
                new Object[]{fileP,
                             aclFileAttributeViewClz,
                             Array.newInstance(linkOptionClz, 0)});

            if (view == null) {
                return false;
            }


            // If we have a posix view, we can use ACLs to interface
            // the usual Unix permission masks vi the special principals
            // OWNER@, GROUP@ and EVERYONE@

            // PosixFileAttributeView posixView =
            // Files.getFileAttributeView(fileP, PosixFileAttributeView.class);
            Object posixView = getFileAttributeView.invoke(
                null,
                new Object[]{fileP,
                             posixFileAttributeViewClz,
                             Array.newInstance(linkOptionClz, 0)});

            // UserPrincipal owner = Files.getOwner(fileP);
            Object owner = getOwner.invoke(
                null,
                new Object[]{fileP, Array.newInstance(linkOptionClz, 0)});

            // List<AclEntry> oldAcl = view.getAcl();
            // List<AclEntry> newAcl = new ArrayList<>();

            List oldAcl = (List)getAcl.invoke(view, null);
            List newAcl = new ArrayList();

            // for (AclEntry ace : oldAcl) {
            //     if (posixView != null) {
            //         if (ace.principal().getName().equals("OWNER@")) {
            //             // retain permission for owner
            //             newAcl.add(ace);
            //         } else if (
            //             ace.principal().getName().equals("GROUP@") ||
            //             ace.principal().getName().equals("EVERYONE@")) {
            //
            //             AclEntry.Builder aceb = AclEntry.newBuilder();
            //             aceb.setPrincipal(ace.principal())
            //                 .setType(AclEntryType.ALLOW);
            //             // add no permissions for the group and other
            //             newAcl.add(aceb.build());
            //         }
            //     } else {
            //         // NTFS, hopefully
            //         if (ace.principal().equals(owner)) {
            //             newAcl.add(ace);
            //         }
            //     }
            // }

            for (Iterator i = oldAcl.iterator(); i.hasNext();) {
                Object ace = i.next();
                Object princ = principal.invoke(ace, null);
                String princName = (String)getName.invoke(princ, null);

                if (posixView != null) {
                    if (princName.equals("OWNER@")) {
                        // retain permission for owner
                        newAcl.add(ace);
                    } else if (
                        princName.equals("GROUP@") ||
                        princName.equals("EVERYONE@")) {

                        // add ALLOW ACE w/no permissions for group and other

                        Object aceb = newBuilder.invoke(null, null);
                        Object allowValue = allow.get(aclEntryTypeClz);

                        aceb = setPrincipal.invoke(aceb, new Object[]{princ});
                        aceb = setType.invoke(aceb, new Object[]{allowValue});
                        newAcl.add(build.invoke(aceb, null));
                    }
                } else {
                    // NTFS, hopefully
                    if (princ.equals(owner)) {
                        newAcl.add(ace);
                    }
                }
            }

            // view.setAcl(newAcl);
            setAcl.invoke(view, new Object[]{newAcl});

        } catch (IllegalAccessException e) {
            // coding error
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(e);
            }
        } catch (IllegalArgumentException e) {
            // coding error
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(e);
            }
        } catch (InvocationTargetException e) {
            // java.security.AccessControlException: access denied
            // ("java.lang.RuntimePermission" "accessUserInformation") can
            // happen, so throw.
            //
            // Should we get an IOException from getOwner, the cast below
            // would throw which is fine, since it should not happen.
            throw (RuntimeException)e.getCause();
        }

        return true;
    }
}

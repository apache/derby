/*

   Derby - Class org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests

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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of operations on {@link java.io.File} that wraps the
 * operations in privileged block of code. This class is intended to provide
 * these methods for testcases to reduce the hassle of having to wrap file
 * operations in privileged code blocks.
 * <p>
 * Derby needs to use privileged blocks in some places to avoid
 * {@link SecurityException}s being thrown, as the required privileges are
 * often granted to Derby itself, but not the higher level application code.
 */
public class PrivilegedFileOpsForTests {

	/**
     * Get the file length.
     *
     * @return Byte length of the file.
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     * @see File#length
     */
    public static long length(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        return AccessController.doPrivileged(
                    new PrivilegedAction<Long>() {
                        public Long run() {
                            return file.length();
                        }
                    });
    }

    /**
     * Get the absolute path
     *
     * @param file File for absolute path
     * @return Absolute path of the file.
     * @throws SecurityException if the required permissions to access the file,
     *      
     * @see File#getAbsolutePath
     */
    public static String getAbsolutePath(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        return AccessController.doPrivileged(
                new PrivilegedAction<String>() {
                    public String run() throws SecurityException {
                        return file.getAbsolutePath();
                    }});
    }

    /**
     * Create a temporary file.
     *
     * @param prefix file name prefix, at least three characters
     * @param suffix file name suffix, defaults to ".tmp" if {@code null}
     * @param directory where to create the file, or {@code null} for the
     *   default temporary file directory
     * @return the file that was created
     * @throws IOException if the file cannot be created
     * @see File#createTempFile(String, String, File)
     */
    public static File createTempFile(final String prefix,
                                      final String suffix,
                                      final File directory)
            throws IOException
    {
        try {
            return AccessController.doPrivileged(
                new PrivilegedExceptionAction<File>() {
                    @Override
                    public File run() throws IOException {
                        return File.createTempFile(prefix, suffix, directory);
                    }
                });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        }
    }

    /**
     * Returns a input stream for the specified file.
     *
     * @param file the file to open a stream for
     * @return A input stream reading from the specified file.
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     * @throws FileNotFoundException if the specified file does not exist
     */
    public static FileInputStream getFileInputStream(final File file) 
            throws FileNotFoundException {
    	if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileInputStream>() {
                public FileInputStream run() throws FileNotFoundException {
                    return new FileInputStream(file);
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (FileNotFoundException)pae.getException();
        }
    }

    /**
     * Check if the file exists.
     *
     * @return {@code true} if file exists, {@code false} otherwise
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     * @see File#exists
     */
    public static boolean exists(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        return AccessController.doPrivileged(
                    new PrivilegedAction<Boolean>() {
                        public Boolean run() {
                            return file.exists();
                        }
                    });
    }

    /**
     * Delete a file
     *
     * @return {@code true} if file was deleted, {@code false} otherwise
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     * @see File#delete
     */
    public static boolean delete(final File file)
            throws SecurityException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        return AccessController.doPrivileged(
                    new PrivilegedAction<Boolean>() {
                        public Boolean run() {
                            return file.delete();
                        }
                    });
    }

    /**
     * Create a directory.
     * @param dir the directory to create
     * @return {@code true} if and only if the directory was created
     */
    public static boolean mkdir(final File dir) {
        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            public Boolean run() {
                return dir.mkdir();
            }
        });
    }

    /**
     * Set the read permission for a file.
     * @param file      the file to set the read permission for
     * @param readable  {@code true} to allow read, {@code false} to deny it
     * @param ownerOnly {@code true} if it should be set for the owner only,
     *                  {@code false} if it should be set for everyone
     * @return {@code true} if the operation succeeded, {@code false} otherwise
     */
    public static boolean setReadable(final File file,
                                      final boolean readable,
                                      final boolean ownerOnly) {
        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            public Boolean run() {
                return file.setReadable(readable, ownerOnly);
            }
        });
    }

    /**
     * Make a file or directory read-only.
     * @param file the file to make read-only
     * @return {@code true} if successful, {@code false} otherwise
     */
    public static boolean setReadOnly(final File file) {
        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                return file.setReadOnly();
            }
        });
    }

    /**
     * Obtains a reader for the specified file.
     *
     * @param file the file to obtain a reader for
     * @return An unbuffered reader for the specified file.
     * @throws FileNotFoundException if the specified file does not exist
     * @throws SecurityException if the required permissions to read the file,
     *      or the path it is in, are missing
     */
    public static FileReader getFileReader(final File file)
            throws FileNotFoundException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileReader>() {
                        public FileReader run()
                                throws FileNotFoundException {
                            return new FileReader(file);
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw (FileNotFoundException)pae.getCause();
        }
    }

    /**
     * Obtains a writer for the specified file.
     *
     * @param file the file to obtain a writer for
     * @return An writer for the specified file.
     * @throws IOException if the file cannot be opened
     * @throws SecurityException if the required permissions to write to the file,
     *      or the path it is in, are missing
     */
    public static FileWriter getFileWriter(final File file)
            throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileWriter>() {
                        public FileWriter run()
                                throws IOException {
                            return new FileWriter(file);
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw (IOException)pae.getCause();
        }
    }

    
    /**
     * In a priv block, do a recursive copy from source to target.  
     * If target exists it will be overwritten. Parent directory for 
     * target will be created if it does not exist. 
     * If source does not exist this will be a noop.
     * 
     * @param source  Source file or directory to copy
     * @param target  Target file or directory to copy to
     * @throws IOException
     * @throws SecurityException
     */    
    public static void copy(final File source, final File target) throws IOException {
        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                public Void run() throws IOException {
                    recursiveCopy(source,target);
                    return null;
                }
                });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getException();
        
        }
        
    }
    /**
     * Do a recursive copy from source to target.  If target exists it will 
     * be overwritten. Parent directory for target will be created if it does
     * not exist. If source does not exist this will be a noop.
     * 
     * @param source  Source file or directory to copy
     * @param target  Target file or directory to copy
     * @throws IOException
     * @throws FileNotFoundException
     */
    private static void  recursiveCopy(File source, File target)
            throws IOException {
    
        // Share the copy buffer between all copy operations.
        byte[] buf = new byte[32*1024];
        if (source.isFile()) {
            copySingleFile(source, target, buf);
            return;
        }
            
        String[] list = source.list();

        // Some JVMs return null for File.list() when the
        // directory is empty.
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                File entry = new File(source, list[i]);
                File targetEntry = new File(target, list[i]);
                if (entry.isDirectory()) {
                    copy(entry,targetEntry);
                } else {
                    copySingleFile(entry, targetEntry, buf);
                }
            }
        }
    }

    /**
     * Copy a single file from source to target.  If target exists it will be 
     * overwritten.  If source does not exist, this will be a noop.
     * 
     * @param source  Source file to copy
     * @param target  Destination file for copy
     * @param buf buffer used for copy (may be {@code null})
     * @throws IOException if accessing the specified files fail
     * @throws FileNotFoundException if a specified file doesn't exist
     */
    private static void copySingleFile (File source, File target, byte[] buf)
            throws IOException {

        // Create a default buffer if necessary.
        if (buf == null) {
            buf = new byte[32 * 1024];
        }
        File targetParent = target.getParentFile();
        if (targetParent != null && ! targetParent.exists())
            target.getParentFile().mkdirs();
        
                
        InputStream in = new FileInputStream(source);
        OutputStream out = new FileOutputStream(target);

        try {
            for (;;) {
                int read = in.read(buf);
                if (read == -1)
                    break;
                out.write(buf, 0, read);
            }
        } finally {
            in.close();
            out.close();
        }
    }
    

    /**
     * Returns a file output stream for the specified file.
     * <p>
     * If the file already exists and is writable, it will be overwritten.
     *
     * @param file the file to create a stream for
     * @return An output stream.
     * @throws FileNotFoundException if the specified file does not exist
     * @throws SecurityException if the required permissions to write the file,
     *      or the path it is in, are missing
     */
    public static FileOutputStream getFileOutputStream(final File file)
            throws FileNotFoundException {
        return getFileOutputStream(file, false);
    }

    /**
     * Returns a file output stream for the specified file.
     *
     * @param file the file to create a stream for
     * @param append whether to append or overwrite an existing file
     * @return An output stream.
     * @throws FileNotFoundException if the specified file does not exist
     * @throws SecurityException if the required permissions to write the file,
     *      or the path it is in, are missing
     */
    public static FileOutputStream getFileOutputStream(final File file,
                                                       final boolean append)
            throws FileNotFoundException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be <null>");
        }
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<FileOutputStream>() {
                        public FileOutputStream run()
                                throws FileNotFoundException {
                            return new FileOutputStream(file, append);
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw (FileNotFoundException)pae.getCause();
        }
    }

    /**
     * Tries to delete all the files, including the specified directory, in the
     * directory tree with the specified root.
     * <p>
     * If deleting one of the files fail, it will be recorded and the method
     * will move on to the remaining files and try to delete them.
     *
     * @param dir the directory to delete (including subdirectories)
     * @return A list of files which couldn't be deleted (may be empty).
     * @see org.apache.derbyTesting.junit.BaseTestCase#assertDirectoryDeleted
     */
    public static File[] persistentRecursiveDelete(final File dir)
            throws FileNotFoundException {
        // Fail if the directory doesn't exist.
        if (!exists(dir)) {
            throw new FileNotFoundException(getAbsolutePath(dir));
        }
        final ArrayList<File> notDeleted = new ArrayList<File>();
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                deleteRecursively(dir, notDeleted);
                return null;
            }
        });

        return notDeleted.toArray(new File[notDeleted.size()]);
    }

    /**
     * Deletes the specified directory and all its files and subdirectories.
     * <p>
     * An attempt is made to delete all files, even if one of the delete
     * operations fail.
     *
     * @param dir the root directory to start deleting from
     * @param failedDeletes a list of failed deletes (if any)
     * @return {@code true} is all delete operations succeeded, {@code false}
     *      otherwise.
     */
    private static boolean deleteRecursively(File dir, List<File> failedDeletes) {
        boolean allDeleted = true;
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (int i=0; i < files.length; i++) {
                    File f = files[i];
                    if (f.isDirectory()) {
                        allDeleted &= deleteRecursively(f, failedDeletes);
                    } else {
                        allDeleted &= internalDelete(f, failedDeletes);
                    }
                }
            }
        }
        allDeleted &= internalDelete(dir, failedDeletes);
        return allDeleted;
    }

    /**
     * Attempts to delete the specified file, will add it to the passed in list
     * if the delete fails.
     *
     * @param f file to delete
     * @param failedDeletes list keeping track of failed deletes
     * @return {@code true} if the delete succeeded, {@code false} otherwise.
     */
    private static boolean internalDelete(File f, List<File> failedDeletes) {
        boolean deleted = f.delete();
        if (!deleted) {
            failedDeletes.add(f);
        }
        return deleted;
    }

    /**
     * Obtains information about the specified file.
     *
     * @param f the file
     * @return A string with file information (human-readable).
     */
    public static String getFileInfo(final File f) {
        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                if (!f.exists()) {
                    return "(non-existant)";
                }
                StringBuilder sb = new StringBuilder();
                sb.append("(isDir=").append(f.isDirectory()).
                        append(", canRead=").append(f.canRead()).
                        append(", canWrite=").append(f.canWrite()).
                        append(", size=").append(f.length()).append(')');
                return sb.toString();
            }
        });
    }
    

 }

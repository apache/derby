/*

   Derby - Class org.apache.derby.impl.services.stream.RollingFileStream

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
package org.apache.derby.impl.services.stream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides rolling file OutputStream.  The file pattern, file size,
 * and number of files can be customized.
 * <p>
 * This class borrows extensively from the java.util.logger.FileHandler class for its
 * file handling ability and instead of handling logger messages it extends 
 * java.io.OutputStream.
 * <p>
 * A pattern consists of a string that includes the following special
 * components that will be replaced at runtime:
 * <ul>
 * <li>    "/"    the local pathname separator 
 * <li>     "%t"   the system temporary directory
 * <li>     "%h"   the value of the "user.home" system property
 * <li>     "%d"   the value of the "derby.system.home" system property
 * <li>     "%g"   the generation number to distinguish rotated logs
 * <li>     "%u"   a unique number to resolve conflicts
 * <li>     "%%"   translates to a single percent sign "%"
 * </ul>
 * If no "%g" field has been specified and the file count is greater
 * than one, then the generation number will be added to the end of
 * the generated filename, after a dot.
 * <p> 
 * Thus for example a pattern of "%t/java%g.log" with a count of 2
 * would typically cause files to be written on Solaris to 
 * /var/tmp/java0.log and /var/tmp/java1.log whereas on Windows 95 they
 * would be typically written to C:\TEMP\java0.log and C:\TEMP\java1.log
 * <p> 
 * Generation numbers follow the sequence 0, 1, 2, etc.
 * <p>
 * Normally the "%u" unique field is set to 0.  However, if the <tt>FileHandler</tt>
 * tries to open the filename and finds the file is currently in use by
 * another process it will increment the unique number field and try
 * again.  This will be repeated until <tt>FileHandler</tt> finds a file name that
 * is  not currently in use. If there is a conflict and no "%u" field has
 * been specified, it will be added at the end of the filename after a dot.
 * (This will be after any automatically added generation number.)
 * <p>
 * Thus if three processes were all trying to output to fred%u.%g.txt then 
 * they  might end up using fred0.0.txt, fred1.0.txt, fred2.0.txt as
 * the first file in their rotating sequences.
 * <p>
 * Note that the use of unique ids to avoid conflicts is only guaranteed
 * to work reliably when using a local disk file system.
 *
 */
public class RollingFileStream extends OutputStream {

    /**
     * The underlying stream being written to that keeps track of how much
     * has been written
     */
    private MeteredStream meter;

    /**
     * The append flag which indicates at creation time to append to an existing
     * file or to always create a new one
     */
    private boolean append;

    /*
     * The file limit.  The value of 0 indicates no limit.
     */
    private int limit;

    /**
     * The rolling file count.  This many files will be created before the
     * oldest is removed and the files rolled.
     */
    private int count;

    /**
     * The filename pattern.
     */
    private String pattern;

    /**
     * The lockfile name
     */
    private String lockFileName;

    /**
     * The output stream that is used as a lock
     */
    private FileOutputStream lockStream;

    /**
     * The array of File instance representing the rolling files
     */
    private File files[];

    private static final int MAX_LOCKS = 100;

    private static java.util.HashMap<String,String> locks = new java.util.HashMap<String,String>();

    /**
     * Construct a default <tt>RollingFileStream</tt>. This will be configured entirely with default values: 
     * <ul>
     * <li>pattern - %d/derby-%g.log (DERBY_HOME/derby-0.log)</li>
     * <li>limit - 0 (unlimited)</li>
     * <li>count - 1 (one file)</li>
     * <li>append - false (overwrite and not append)</li>
     * </ul>
     *
     * @exception IOException if there are IO problems opening the files.
     * @exception SecurityException if a security manager exists and if the caller does not have
     * <tt>LoggingPermission("control"))</tt>.
     * @exception NullPointerException if pattern property is an empty String.
     */
    public RollingFileStream() throws IOException, SecurityException {
        this("%d/derby-%g.log", 0, 1, false);
    }

    /**
     * Initialize a <tt>RollingFileStream</tt> to write to a set of files with optional append. When (approximately) the
     * given limit has been written to one file, another file will be opened. The output will cycle through a set of
     * count files.
     *
     * @param pattern the pattern for naming the output file
     * @param limit the maximum number of bytes to write to any one file
     * @param count the number of files to use
     * @param append specifies append mode
     * @exception IOException if there are IO problems opening the files.
     * @exception SecurityException if a security manager exists and if the caller does not have
     * <tt>LoggingPermission("control")</tt>.
     * @exception IllegalArgumentException if limit &lt; 0, or count &lt; 1.
     * @exception IllegalArgumentException if pattern is an empty string
     *
     */
    public RollingFileStream(String pattern, int limit, int count, boolean append)
            throws IOException, SecurityException {
        if (limit < 0 || count < 1 || pattern.length() < 1) {
            throw new IllegalArgumentException();
        }
        this.pattern = pattern;
        this.limit = limit;
        this.count = count;
        this.append = append;
        openFiles();
    }

    /**
     * Implements the write method of the OutputStream.  This writes the value
     * to the metered stream.
     * @param b The value to write
     * @throws IOException 
     */
    public void write(int b) throws IOException {
        this.meter.write(b);
        checkMeter();
    }

    /**
     * Opens the output files files based on the configured pattern, limit, count,
     * and append mode.
     * @throws IOException 
     */
    private void openFiles() throws IOException {
        if (count < 1) {
            throw new IllegalArgumentException("file count = " + count);
        }
        if (limit < 0) {
            limit = 0;
        }

        // Create a lock file.  This grants us exclusive access
        // to our set of output files, as long as we are alive.
        int unique = -1;
        for (;;) {
            unique++;
            if (unique > MAX_LOCKS) {
                throw new IOException("Couldn't get lock for " + pattern);
            }
            // Generate a lock file name from the "unique" int.
            lockFileName = generate(pattern, 0, unique).toString() + ".lck";
            // Now try to lock that filename.
            // Because some systems (e.g. Solaris) can only do file locks
            // between processes (and not within a process), we first check
            // if we ourself already have the file locked.
            synchronized (locks) {
                if (locks.get(lockFileName) != null) {
                    // We already own this lock, for a different RollingFileStream
                    // object.  Try again.
                    continue;
                }
                FileChannel fc;
                try {
                    lockStream = openFile(lockFileName, false);
                    fc = lockStream.getChannel();
                } catch (IOException ix) {
                    // We got an IOException while trying to open the file.
                    // Try the next file.
                    continue;
                }
                try {
                    FileLock fl = fc.tryLock();
                    if (fl == null) {
                        // We failed to get the lock.  Try next file.
                        continue;
                    }
                    // We got the lock OK.
                } catch (IOException ix) {
                    // We got an IOException while trying to get the lock.
                    // This normally indicates that locking is not supported
                    // on the target directory.  We have to proceed without
                    // getting a lock.   Drop through.
                }
                // We got the lock.  Remember it.
                locks.put(lockFileName, lockFileName);
                break;
            }
        }

        files = new File[count];
        for (int i = 0; i < count; i++) {
            files[i] = generate(pattern, i, unique);
        }

        // Create the initial log file.
        if (append) {
            open(files[0], true);
        } else {
            rotate();
        }
    }

    /**
     * Generates and returns File from a pattern
     * @param pattern The filename pattern
     * @param generation The generation number used if there is a conflict
     * @param unique The unique number to append to the filename
     * @return The File
     * @throws IOException 
     */
    private File generate(String pattern, int generation, int unique) throws IOException {
        File file = null;
        String word = "";
        int ix = 0;
        boolean sawg = false;
        boolean sawu = false;
        while (ix < pattern.length()) {
            char ch = pattern.charAt(ix);
            ix++;
            char ch2 = 0;
            if (ix < pattern.length()) {
                ch2 = Character.toLowerCase(pattern.charAt(ix));
            }
            if (ch == '/') {
                if (file == null) {
                    file = new File(word);
                } else {
                    file = new File(file, word);
                }
                word = "";
                continue;
            } else if (ch == '%') {
                if (ch2 == 't') {
                    String tmpDir = getSystemProperty("java.io.tmpdir");
                    if (tmpDir == null) {
                        tmpDir = getSystemProperty("user.home");
                    }
                    file = new File(tmpDir);
                    ix++;
                    word = "";
                    continue;
                } else if (ch2 == 'h') {
                    file = new File(getSystemProperty("user.home"));
                    ix++;
                    word = "";
                    continue;
                } else if (ch2 == 'd') {
                    String derbyHome = getSystemProperty("derby.system.home");
                    if (derbyHome == null) {
                        derbyHome = getSystemProperty("user.dir");
                    }
                    file = new File(derbyHome);
                    ix++;
                    word = "";
                    continue;
                } else if (ch2 == 'g') {
                    word = word + generation;
                    sawg = true;
                    ix++;
                    continue;
                } else if (ch2 == 'u') {
                    word = word + unique;
                    sawu = true;
                    ix++;
                    continue;
                } else if (ch2 == '%') {
                    word = word + "%";
                    ix++;
                    continue;
                }
            }
            word = word + ch;
        }
        if (count > 1 && !sawg) {
            word = word + "." + generation;
        }
        if (unique > 0 && !sawu) {
            word = word + "." + unique;
        }
        if (word.length() > 0) {
            if (file == null) {
                file = new File(word);
            } else {
                file = new File(file, word);
            }
        }
        return file;
    }

    /**
     * Rotates the log files.  The metered OutputStream is closed,the log files
     * are rotated and then a new metered OutputStream is created.
     * @throws IOException 
     */
    private synchronized void rotate() throws IOException {

        if (null != meter) {
            meter.close();
        }
        for (int i = count - 2; i >= 0; i--) {
            File f1 = files[i];
            File f2 = files[i + 1];
            if (fileExists(f1)) {
                if (fileExists(f2)) {
                    fileDelete(f2);
                }
                fileRename(f1, f2);
            }
        }
        try {
            open(files[0], false);
        } catch (IOException ix) {
        }
    }

    /**
     * Close all the files.
     *
     * @exception SecurityException if a security manager exists and if the caller does not have
     * <tt>LoggingPermission("control")</tt>.
     */
    public synchronized void close() throws SecurityException {
        // Close the underlying file
        if (null != meter) {
            try {
                meter.close();
            } catch (IOException ex) {
                // Problems closing the stream.  Punt.S
            }
        }
        // Unlock any lock file.
        if (lockFileName == null) {
            return;
        }
        try {
            // Closing the lock file's FileOutputStream will close
            // the underlying channel and free any locks.
            lockStream.close();
        } catch (Exception ex) {
            // Problems closing the stream.  Punt.S
        }
        synchronized (locks) {
            locks.remove(lockFileName);
        }
        fileDelete(new File(lockFileName));
        lockFileName = null;
        lockStream = null;
    }

    /**
     * Gets a system property in a privileged block
     * @param property The propety to get
     * @return The property value
     */
    private String getSystemProperty(final String property) {
        // Try to get the derby.system.home property.  This requires privileges if run with a security manager
        String value = AccessController.doPrivileged(new PrivilegedAction<String>() {

            public String run() {
                return System.getProperty(property);
            }

        });

        return value;
    }

    /**
     * Opens a file in the privileged block
     * @param filename The name of the file to open
     * @param append if <code>true</code> open the file in append mode
     * @return The FileOutputStream for the file
     * @throws IOException 
     */
    private FileOutputStream openFile(final String filename, final boolean append) throws IOException {
        FileOutputStream fis = null;
        try {
            fis = AccessController.doPrivileged(new PrivilegedExceptionAction<FileOutputStream>() {

                public FileOutputStream run() throws FileNotFoundException {
                    FileOutputStream res = new FileOutputStream(filename, append);
                    return res;
                }

            });
            return fis;
        } catch (PrivilegedActionException x) {
            // An exception occurs, rethrow it
            throw (IOException) x.getException();
        }
    }
    
    /**
     * Check to see if a file exists in a privilege block
     * @param file The file to check
     * @return <code>true</code> if the file exists or <code>false</code> otherwise
     */
    private boolean fileExists(final File file) {
        // Try to get the derby.system.home property.  This requires privileges if run with a security manager
        Boolean value = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {

            public Boolean run() {
                return file.exists();
            }

        });

        return value.booleanValue();        
    }

    /**
     * Delete a file in a privilege block
     * @param file The file to delete
     */
    private void fileDelete(final File file) {
        // Try to get the derby.system.home property.  This requires privileges if run with a security manager
        AccessController.doPrivileged(new PrivilegedAction<Object>() {

            public Object run() {
                file.delete();
                return null;
            }
        });
    }

    /**
     * Rename a file in a privilege block
     * @param file1 The file to rename
     * @param file2 The file to rename it to
     * @return <code>true</code> if the file was renamed or </code>false</code> otherwise
     */
    private boolean fileRename(final File file1, final File file2) {
        // Try to get the derby.system.home property.  This requires privileges if run with a security manager
        Boolean value = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {

            public Boolean run() {
                return file1.renameTo(file2);
            }
        });

        return value.booleanValue();        
    }
    
    /**
     * Get the length of a file in a privilege block
     * @param file The file to get the length of
     * @return The length of the file
     */
    private long fileLength(final File file) {
        // Try to get the derby.system.home property.  This requires privileges if run with a security manager
        Long value = AccessController.doPrivileged(new PrivilegedAction<Long>() {

            public Long run() {
                return file.length();
            }
        });

        return value.longValue();                
    }

    /**
     * Opens a new file that and delegates it to a MeteredStream
     * @param fname The name of the file
     * @param append If <code>true</code> append to the existing file
     * @throws IOException 
     */
    private void open(File fname, boolean append) throws IOException {
        int len = 0;
        if (append) {
            len = (int) fileLength(fname);
        }
        FileOutputStream fout = openFile(fname.toString(), append);
        meter = new MeteredStream(fout, len);
    }

    /**
     * Invoked by the metered OutputStream 
     * @throws IOException 
     */
    private void checkMeter() throws IOException {
        if (limit > 0 && meter.written >= limit) {
            rotate();
        }
    }

    // A metered stream is a subclass of OutputStream that
    //   (a) forwards all its output to a target stream
    //   (b) keeps track of how many bytes have been written
    private class MeteredStream extends OutputStream {

        /**
         *  The real OutputStream to delegate to
         */
        OutputStream out;

        /**
         * The number of bytes written so far to the OutputStream
         */
        int written;

        /**
         * Creates a new instance of MeteredStream
         * @param out The OutputStream to delegate to
         * @param written The number of bytes currently written to the OuptutStream
         */
        MeteredStream(OutputStream out, int written) {
            this.out = out;
            this.written = written;
        }

        public void write(int b) throws IOException {
            out.write(b);
            written++;
        }
        
        public int getWritten() {
            return written;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }        
    }
}

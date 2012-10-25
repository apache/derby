/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.VirtualFileTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.impl.io.vfmem.DataStore;
import org.apache.derby.impl.io.vfmem.VirtualFile;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Basic tests of the class {@code VirtualFile}.
 */
public class VirtualFileTest
        extends BaseTestCase {

    private final String[] NON_EXISTING_DIRS = new String[] {
                "this", "dir", "does", "not", "exist"};

    public VirtualFileTest(String name) {
        super(name);
    }

    public void testCreateFileInRoot() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("service.properties", store);
        assertFalse(new VirtualFile("service.properties", store).exists());
        assertFalse(vFile.exists());
    }

    public void testCreateDirInRoot() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("seg0", store);
        assertFalse(vFile.exists());
        vFile.mkdir();
        assertTrue(vFile.exists());
        assertTrue(vFile.isDirectory());
    }

    public void testCreateInvalidDir() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.join(NON_EXISTING_DIRS),
                store);
        assertFalse(vFile.exists());
        VirtualFile tmp = new VirtualFile("", store);
        assertTrue(tmp.mkdir());
        assertFalse("Dir creation should have failed", vFile.mkdir());
    }

    public void testMkdirsValidRelative() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.join(NON_EXISTING_DIRS),
                store);
        assertTrue(vFile.mkdirs());
    }

    public void testMkdirsValidAbsolute() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.joinAbs(NON_EXISTING_DIRS),
                store);
        assertTrue(vFile.mkdirs());
    }

    public void testMkdirsInvalidAbsolute()
            throws IOException {
        DataStore store = getStore();
        VirtualFile tmp = new VirtualFile(PathUtilTest.abs("directory"), store);
        assertTrue(tmp.mkdir());
        tmp = new VirtualFile(
                PathUtilTest.joinAbs("directory", "afile"),
                store);
        assertTrue(tmp.createNewFile());
        assertTrue(tmp.exists());
        assertFalse(tmp.isDirectory());
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.joinAbs("directory", "afile", "anotherdir"),
                store);
        assertFalse(vFile.mkdir());
        assertFalse(vFile.mkdirs());
    }

    public void testMkdirsInvalidRelative()
            throws IOException {
        DataStore store = getStore();
        VirtualFile tmp = new VirtualFile("seg0", store);
        assertTrue(tmp.mkdir());
        tmp = new VirtualFile(PathUtilTest.join("seg0", "datafile"), store);
        assertTrue(tmp.createNewFile());
        assertTrue(tmp.exists());
        assertFalse(tmp.isDirectory());
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.join("seg0", "datafile", "anotherdir"), store);
        assertFalse(vFile.mkdir());
        assertFalse(vFile.mkdirs());
    }

    public void testGetParentRelative() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.join(NON_EXISTING_DIRS), store);
        int count = 0;
        StorageFile parent = vFile.getParentDir();
        while (parent != null) {
            count++;
            parent = parent.getParentDir();
        }
        assertEquals(4, count);
    }

    public void testGetParentAbsolute() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile(
                PathUtilTest.joinAbs(NON_EXISTING_DIRS), store);
        int count = 0;
        StorageFile parent = vFile.getParentDir();
        while (parent != null) {
            count++;
            parent = parent.getParentDir();
        }
        assertEquals(5, count);
    }

    public void testDeleteAll()
            throws IOException {
        DataStore store = getStore();
        String[] dirs = new String[] {
            "seg0", PathUtilTest.join("seg0", "dir1"),
            "seg1", PathUtilTest.join("seg0", "dir2")};
        for (int i=0; i < dirs.length; i++) {
            assertTrue(new VirtualFile(dirs[i], store).mkdir());
        }
        String[] files = new String[] {
            PathUtilTest.join("seg0", "f1"),
            PathUtilTest.join("seg0", "dir1", "f1"),
            PathUtilTest.join("seg1", "f1"), PathUtilTest.join("seg0","f5")};
        for (int i=0; i < files.length; i++) {
            assertTrue(new VirtualFile(files[i], store).createNewFile());
        }
        String root = "seg0";
        VirtualFile rootToDelete = new VirtualFile(root, store);
        assertTrue(rootToDelete.deleteAll());
        for (int i=0; i < dirs.length; i++) {
            assertEquals(!dirs[i].startsWith(root),
                         new VirtualFile(dirs[i], store).exists());
        }
        for (int i=0; i < files.length; i++) {
            assertEquals(!files[i].startsWith(root),
                         new VirtualFile(files[i], store).exists());
        }
    }

    public void testRenameToSimple() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("originalFile", store);
        assertFalse(vFile.canWrite());
        vFile.createNewFile();
        assertTrue(vFile.canWrite());
        VirtualFile newFile = new VirtualFile("newFile", store);
        assertFalse(newFile.exists());
        assertTrue(vFile.renameTo(newFile));
        assertFalse(vFile.exists());
        assertFalse(vFile.canWrite());
        assertTrue(newFile.exists());
    }

    /**
     * Getting a random access file in write mode for a non-existing file
     * should cause the file to be created.
     */
    public void testGetRAFNonExisting()
            throws FileNotFoundException {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("aNewFile.txt", store);
        assertFalse(vFile.exists());
        StorageRandomAccessFile vRAF = vFile.getRandomAccessFile("rw");
        assertNotNull(vRAF);
        assertTrue(vFile.exists());
    }

    /**
     * Getting a random access file in read mode for a non-existing file
     * should fail, and the file shouldn't be created.
     */
    public void testGetRAFNonExistingReadMode()
            throws FileNotFoundException {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("aNewFile.txt", store);
        assertFalse(vFile.exists());
        try {
            vFile.getRandomAccessFile("r");
            fail("Cannot read from a non-exsiting file");
        } catch (FileNotFoundException fnfe) {
            // Expected.
        }
        assertFalse(vFile.exists());
    }

    /**
     * Opens a random access file for a file which has been marked as read-only.
     * <p>
     * Opening for reading only should work, opening for writing should fail.
     */
    public void testGetRAExistingReadOnly()
            throws FileNotFoundException {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("aNewFile.txt", store);
        assertFalse(vFile.exists());
        assertTrue(vFile.createNewFile());
        assertTrue(vFile.exists());
        assertTrue(vFile.setReadOnly());
        assertNotNull(vFile.getRandomAccessFile("r"));
        // Try opening in write mode, which should fail.
        try {
            vFile.getRandomAccessFile("rw");
            fail("Should not be able to open a read-only file in write mode");
        } catch (FileNotFoundException fnfe) {
            // Expected.
        }
    }

    /**
     * Opening a random access file on a directory should fail.
     */
    public void testGetRAFOnDirectory() {
        DataStore store = getStore();
        VirtualFile vFile = new VirtualFile("mydir", store);
        assertTrue(vFile.mkdir());
        assertTrue(vFile.exists());
        assertTrue(vFile.isDirectory());
        // Try opening in read mode.
        try {
            vFile.getRandomAccessFile("r");
            fail("Opening a RAF on a directory should have failed");
        } catch (FileNotFoundException fnfe) {
            // Expected.
        }
        // Try opening in write mode.
        try {
            vFile.getRandomAccessFile("r");
            fail("Opening a RAF on a directory should have failed");
        } catch (FileNotFoundException fnfe) {
            // Expected.
        }
        // A few sanity checks.
        assertTrue(vFile.exists());
        assertTrue(vFile.isDirectory());
    }

    /**
     * Tests that {@code listChildren} doesn't include too many entries.
     */
    public void testListChilderen() {
        DataStore store = getStore();
        VirtualFile dir1 = new VirtualFile(PathUtilTest.abs("mydir"), store);
        VirtualFile dir2 = new VirtualFile(
                PathUtilTest.abs("mydirectory"), store);
        VirtualFile file1 = new VirtualFile(
                PathUtilTest.joinAbs("mydir", "file1.txt"), store);
        VirtualFile file2 = new VirtualFile(
                PathUtilTest.joinAbs("mydirectory", "file2.txt"), store);
        assertTrue(dir1.mkdirs());
        assertTrue(dir1.exists());
        assertTrue(dir1.isDirectory());
        assertTrue(dir2.mkdirs());
        assertTrue(dir2.exists());
        assertTrue(dir2.isDirectory());
        assertTrue(file1.createNewFile());
        assertTrue(file1.exists());
        assertFalse(file1.isDirectory());
        assertTrue(file2.createNewFile());
        assertTrue(file2.exists());
        assertFalse(file2.isDirectory());
        // We should only get one child; file1.txt
        String[] children = dir1.list();
        assertEquals(1, children.length);
        assertEquals(file1.getName(), children[0]);
        // Test that the same path ending with the separator results in the
        // same list being returned.
        VirtualFile dir1abs = new VirtualFile(
                PathUtilTest.joinAbs("mydir", ""), store);
        assertFalse(dir1.getName().equals(dir1abs.getName()));
        String[] childrenAbs = dir1abs.list();
        assertEquals(1, childrenAbs.length);
        assertEquals(children[0], childrenAbs[0]);
        // The deleteAll below shouldn't delete "mydirectory" and "file2.txt"..
        assertFalse(dir1.delete());
        assertTrue(dir1.deleteAll());
        assertTrue(dir2.exists());
        assertTrue(file2.exists());
    }

    /**
     * Makes sure that the root can be created.
     */
    public void testCreateRoot() {
        DataStore store = new DataStore("testCreateRootStore");
        String path = PathUtilTest.joinAbs("these", "are", "directories");
        assertTrue(store.createAllParents(path));
        assertNotNull(store.createEntry(path, true));
        VirtualFile vf = new VirtualFile(path, store);
        assertTrue(vf.exists());
        assertTrue(vf.isDirectory());

        // Also test one Windows specific root.
        path = PathUtilTest.join("c:", "Documents and Settings", "directories");
        assertTrue(store.createAllParents(path));
        assertNotNull(store.createEntry(path, true));
        vf = new VirtualFile(path, store);
        assertTrue(vf.exists());
        assertTrue(vf.isDirectory());
    }

    /**
     * Verify that the close() method of VirtualRandomAccessFile can be
     * called more than once.
     */
    public void testCloseIdempotent() throws IOException {
        DataStore store = getStore();
        VirtualFile f = new VirtualFile("afile", store);
        StorageRandomAccessFile raf = f.getRandomAccessFile("rw");
        raf.close();
        // The second close() used to throw NullPointerException (DERBY-5960)
        raf.close();
    }

    public static Test suite() {
        return new TestSuite(VirtualFileTest.class);
    }

    /** A counter used to obtain unique data store names. */
    private static int dbStoreIndex = 0;
    /** Utility method returning a fresh data store. */
    private static synchronized DataStore getStore() {
        DataStore store = new DataStore("testVFMemDB-" + dbStoreIndex++);
        // We need the root to exist.
        assertNotNull(store.createEntry(java.io.File.separator, true));
        return store;
    }
}

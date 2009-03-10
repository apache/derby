/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.PathUtilTest

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

import java.io.File;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.impl.io.vfmem.PathUtil;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Basic tests for the {@code PathUtil} class.
 */
public class PathUtilTest
        extends BaseTestCase {

    private static final String SEP = File.separator;

    public PathUtilTest(String name) {
        super(name);
    }

    public void testGetParent() {
        assertNull(PathUtil.getParent(""));
        assertNull(PathUtil.getParent(File.separator));
        assertEquals("seg0",
                PathUtil.getParent(join("seg0","c1.dat")));
        assertEquals(abs("seg0"),
                PathUtil.getParent(joinAbs("seg0", "c1.dat")));
        assertNull(PathUtil.getParent("seg0" + SEP));
        assertEquals(SEP,
                PathUtil.getParent(abs("seg0" + SEP)));
        assertEquals(joinAbs("dir1", "dir2"),
                PathUtil.getParent(joinAbs("dir1", "dir2", "myFile.txt")));
    }

    public void testGetBase() {
        assertEquals("seg0", PathUtil.getBaseName("seg0"));
        assertEquals("c1.dat",
                PathUtil.getBaseName(join("seg0","c1.dat")));
        assertEquals("c1.dat",
                PathUtil.getBaseName(joinAbs("seg0","c1.dat")));
        assertEquals("c1.dat",
                PathUtil.getBaseName(join("aDir", "seg0","c1.dat")));
        assertEquals("c1.dat",
                PathUtil.getBaseName(joinAbs("aDir", "seg0","c1.dat")));
    }

    public static Test suite() {
        return new TestSuite(PathUtilTest.class, "PathUtilTest suite");
    }

    // Simple utility methods to join / create paths.

    public static String abs(String e1) {
        return SEP + e1;
    }
    public static String join(String e1, String e2) {
        return e1 + SEP + e2;
    }

    public static String joinAbs(String e1, String e2) {
        return SEP + join(e1, e2);
    }

    public static String join(String e1, String e2, String e3) {
        return e1 + SEP + e2 + SEP + e3;
    }

    public static String joinAbs(String e1, String e2, String e3) {
        return SEP + join(e1, e2, e3);
    }

    public static String join(String[] elems) {
        StringBuffer str = new StringBuffer();
        for (int i=0; i < elems.length; i++) {
            str.append(elems[i]);
            str.append(SEP);
        }
        str.deleteCharAt(str.length() -1);
        return str.toString();
    }

    public static String joinAbs(String[] elems) {
        return SEP + join(elems);
    }
}

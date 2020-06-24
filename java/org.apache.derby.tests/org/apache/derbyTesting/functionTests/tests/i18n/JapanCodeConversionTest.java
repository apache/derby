/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.i18n.JapanCodeConversionTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that files encoded in EUC-JP and SJIS can be imported from and
 * exported to.
 */
public class JapanCodeConversionTest extends BaseJDBCTestCase {
    public JapanCodeConversionTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test requires support for specific encodings which are
        // not guaranteed to be supported by all JVMs. Run the test only
        // if the encodings are supported.
        if (Charset.isSupported("EUC_JP") && Charset.isSupported("SJIS")) {
            return new SupportFilesSetup(
                TestConfiguration.embeddedSuite(JapanCodeConversionTest.class),
                new String[] { "functionTests/tests/i18n/data/jap_EUC_JP.dat" },
                (String[]) null);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        return new BaseTestSuite(
            "JapanCodeConversionTest - skipped because of " +
            "missing support for EUC_JP and SJIS");
    }

    protected void tearDown() throws Exception {
        dropTable("T1_EUC_JP");
        dropTable("T1_EUC_JP_IMPORT_AS_EUC_JP");
        dropTable("T1_EUC_JP_IMPORT_AS_SJIS");
        super.tearDown();
    }

    /**
     * The expected test data. Should match the rows in the jap_EUC_JP.dat
     * file, from which we import data into the test tables.
     */
    private static final String[][] TABLE = {
        {"1", "15:32:06", "\u30a4\u30d9\u30f3\u30c8\u30a2\u30e9\u30fc\u30e0\u304c\u6709\u52b9\u3067\u3059\u3002"},
        {"2", "15:32:10", "DR:DRAUTO\u306f0 (Off)\u3067\u3059\u3002"},
        {"3", "15:32:28", "INFORMIX-OnLine\u304c\u521d\u671f\u5316\u3055\u308c\u3001\u30c7\u30a3\u30b9\u30af\u306e\u521d\u671f\u5316\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002"},
        {"4", "15:32:29", "\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 0\u79d2\u3067\u3057\u305f"},
        {"5", "15:32:29", "\u3059\u3079\u3066\u306eDB\u9818\u57df\u306e\u30c7\u30fc\u30bf\u30b9\u30ad\u30c3\u30d7\u306f\u73fe\u5728\u30aa\u30d5\u306b\u306a\u3063\u3066\u3044\u307e\u3059\u3002"},
        {"6", "15:32:30", "On-Line\u30e2\u30fc\u30c9"},
        {"7", "15:32:31", "sysmaster\u30c7\u30fc\u30bf\u30d9\u30fc\u30b9\u3092\u4f5c\u6210\u4e2d\u3067\u3059..."},
        {"8", "15:33:22", "\u8ad6\u7406\u30ed\u30b0 1\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002"},
        {"9", "15:33:23", "\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 1\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406"},
        {"10", "15:33:40", "\u8ad6\u7406\u30ed\u30b0 2\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002"},
        {"11", "15:33:41", "\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 2\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406"},
        {"12", "15:33:43", "\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 2\u79d2\u3067\u3057\u305f"},
        {"13", "15:34:29", "\u8ad6\u7406\u30ed\u30b0 3\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002"},
        {"14", "15:34:30", "\u30ea\u30bf\u30fc\u30f3\u30b3\u30fc\u30c9 1\u3092\u623b\u3057\u3066\u30d7\u30ed\u30bb\u30b9\u304c\u7d42\u4e86\u3057\u307e\u3057\u305f:/bin/sh /bin/sh -c /work1/MOSES_7.22.UC1A5_27/sqldist/etc/log_full.sh 2 23 \u8ad6\u7406\u30ed\u30b0 3\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002 \u8ad6\u7406"},
        {"15", "15:35:35", "sysmaster\u30c7\u30fc\u30bf\u30d9\u30fc\u30b9\u306e\u4f5c\u6210\u306f\u5b8c\u4e86\u3057\u307e\u3057\u305f\u3002"},
        {"16", "15:39:10", "\u30c1\u30a7\u30c3\u30af\u30dd\u30a4\u30f3\u30c8\u304c\u5b8c\u4e86\u3057\u307e\u3057\u305f:\u7d99\u7d9a\u6642\u9593\u306f 8\u79d2\u3067\u3057\u305f"},
    };

    /**
     * Import data from the EUC-JP-encoded file jap_EUC_JP.dat, and verify
     * that the data is as expected. Then export the data to files encoded
     * in EUC-JP and SJIS, and verify that these files can be imported from.
     */
    public void testImportExport() throws SQLException {
        CallableStatement imp = prepareCall(
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE(null, ?, ?, null, null, ?, 0)");

        CallableStatement exp = prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE(null, ?, ?, null, null, ?)");

        Statement s = createStatement();

        // table for data in EUC_JP encoding
        s.execute("create table T1_EUC_JP"
                + "(jnum int, jtime time, jstring char(200))");

        // import data in EUC_JP encoding
        imp.setString(1, "T1_EUC_JP");
        imp.setString(2, "extin/jap_EUC_JP.dat");
        imp.setString(3, "EUC_JP");
        assertUpdateCount(imp, 0);

        // verify imported data
        JDBC.assertFullResultSet(
            s.executeQuery("SELECT * FROM T1_EUC_JP ORDER BY jnum"),
            TABLE);

        // export to file with EUC_JP encoding
        exp.setString(1, "T1_EUC_JP");
        exp.setString(2, "extout/jap_EUC_JP.dump");
        exp.setString(3, "EUC_JP");
        assertUpdateCount(exp, 0);

        // export to file with SJIS encoding
        exp.setString(1, "T1_EUC_JP");
        exp.setString(2, "extout/jap_SJIS.dump");
        exp.setString(3, "SJIS");
        assertUpdateCount(exp, 0);

        // import as EUC_JP and compare to original
        s.execute("create table T1_EUC_JP_IMPORT_AS_EUC_JP"
                + "(jnum int, jtime time, jstring char(200))");
        imp.setString(1, "T1_EUC_JP_IMPORT_AS_EUC_JP");
        imp.setString(2, "extout/jap_EUC_JP.dump");
        imp.setString(3, "EUC_JP");
        assertUpdateCount(imp, 0);

        // verify imported data
        JDBC.assertFullResultSet(
            s.executeQuery(
                "SELECT * FROM T1_EUC_JP_IMPORT_AS_EUC_JP ORDER BY jnum"),
            TABLE);

        // import as SJIS and compare to original
        s.execute("create table T1_EUC_JP_IMPORT_AS_SJIS"
                + "(jnum int, jtime time, jstring char(200))");
        imp.setString(1, "T1_EUC_JP_IMPORT_AS_SJIS");
        imp.setString(2, "extout/jap_SJIS.dump");
        imp.setString(3, "SJIS");
        assertUpdateCount(imp, 0);

        // verify imported data
        JDBC.assertFullResultSet(
            s.executeQuery(
                "SELECT * FROM T1_EUC_JP_IMPORT_AS_SJIS ORDER BY jnum"),
            TABLE);
    }
}

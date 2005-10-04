/*
  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.errorStream

  Copyright 2001, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/*
 * Partial test of semantics for the three derby.stream.error.* flags.
 * See their description in the Tuning Guide. 
 * Limitations:
 *       - The test uses only OutputStream values for .field and .method
 *         (not Writer, which is also allowed)
 *       - Negative test don't exercise all ways to fail; the test uses
 *         non-existence, but not non-accessability
 *       - Tests precedence, but only for valid values (e.g. missing: "what if
 *         non-existing file is specified AND also a method or field": 
 *         Fallback should be System.err )
 * 
 */


final class AssertException extends Exception 
{
   AssertException (String e) {
      super(e);
   };
}

public class errorStream
{
   private static String derbyHome;

   private static Properties sysProps;
   private static final String FILE_PROP   = "derby.stream.error.file";
   private static final String METHOD_PROP = "derby.stream.error.method";
   private static final String FIELD_PROP  = "derby.stream.error.field";

   /*
    * database names are constructed as <database>-<runNo>
    */
   private static final String database = "VombatusUrsinusHirsutus";

   /* runNo keeps track of which run we are in: Derby is booted
    * several times; once for each combination of properties we want
    * to test the behavior of.
    */ 
   private static int runNo = 0;

   /*
    * File used when FILE_PROP is set, it maps to file
    * <database>-file-<runNo>.log
    */
   private static File fileStreamFile;

   /* see doc for getStream below */
   private static OutputStream methodStream;
   private static File methodStreamFile;

   /*
    * Field fieldStream used by Derby when FIELD_PROP is set, 
    * so it needs to be public.  Maps to file <database>-field-<runNo>.log
    */
   public static OutputStream fieldStream;
   private static File fieldStreamFile;

   /*
    * Field errStream used as redirection for System.err to be able
    * to checks its (non-)use in the scenarios. We first tried to
    * merge it with System.out and let the harness compare outputs,
    * but this gave intermittent merging differences, so abandoned.
    * Maps to file <database>-err-<runNo>.log
    */
   private static OutputStream errStream;
   private static File errStreamFile;

   /*
    * Method getStream used by Derby when METHOD_PROP
    * is set.  Maps to file <database>-method-<runNo>.log
    */
   public static OutputStream getStream() {
      return methodStream;
   }

   private static String makeStreamFilename(String type) {
      return database + "-" + type + "-" + runNo + ".log";
   }

   private static String makeDatabaseName() {
      return database + "-" + runNo;
   }

   private static void openStreams() throws IOException{

      runNo += 1;

      try {
         fileStreamFile = new File(derbyHome, makeStreamFilename("file"));

         methodStreamFile = new File(derbyHome, makeStreamFilename("method"));
         methodStream = new FileOutputStream(methodStreamFile);

         fieldStreamFile = new File(derbyHome, makeStreamFilename("field"));
         fieldStream = new FileOutputStream(fieldStreamFile);

	 errStreamFile = new File(derbyHome, makeStreamFilename("err"));
	 errStream =new FileOutputStream(errStreamFile);
	 System.setErr(new PrintStream(errStream));
      }
      catch (IOException e) {
         System.out.println("Could not open stream files");
         throw e;
      }
   }


   private static void closeStreams() throws IOException {
      try {
         methodStream.close();
         fieldStream.close();
	 errStream.close();

	 // reset until next scenario, no expected output
	 System.setErr(System.out); 
      }
      catch (IOException e) {
         System.out.println("Could not close stream files");
         throw e;
      }
   }


   private static void assertEmpty(File f) throws AssertException, 
                                                  IOException {
      if ( ! (f.exists() && (f.length() == 0)) ) {
         AssertException e = new AssertException("assertEmpty failed: : " + 
                                                 f.getCanonicalPath());
         throw e;
      }
   }


   private static void assertNonEmpty(File f) throws AssertException, 
                                                     IOException {
      if ( ! f.exists() || (f.length() == 0) ) {
         AssertException e = new AssertException("assertNonEmpty failed:" + 
                                                 f.getCanonicalPath());
         throw e;
      }
   }


   private static void assertNonExisting(File f) throws AssertException, 
                                                        IOException {
      if ( f.exists() ) {
         AssertException e = new AssertException("assertNonExisting failed: " + 
                                                 f.getCanonicalPath());
         throw e;
      }
   }


   private static void resetProps () {
      sysProps.remove(FILE_PROP);
      sysProps.remove(METHOD_PROP);
      sysProps.remove(FIELD_PROP);
   }


   private static void bootDerby () throws SQLException {
      Properties attrs = new Properties();
      attrs.setProperty("databaseName", makeDatabaseName());
      attrs.setProperty("createDatabase", "create");
      DataSource ds = TestUtil.getDataSource(attrs);
      try {
         Connection conn = ds.getConnection();
         conn.close();
      }
      catch (SQLException e) {
         System.out.println("Derby boot failed: " +
			    attrs.getProperty("databaseName") + " : " +
			    e.getSQLState() + ": " + e.getMessage());
         throw e;
      }
   }


   private static void shutdownDerby () throws AssertException, SQLException {
      Properties attrs = new Properties();
      attrs.setProperty("databaseName", "");
      attrs.setProperty("shutdownDatabase", "shutdown");
      DataSource ds = TestUtil.getDataSource(attrs);
      try {
         Connection conn = ds.getConnection();
         AssertException e = new AssertException("shutdown failed: " + 
                                                 makeDatabaseName());
         throw e;
      }
      catch (SQLException e) {
         System.out.println("shutdown ok: " + e.getSQLState() + ":" + 
                            e.getMessage());
      }
   }


   private static void checkFile() throws AssertException, IOException, 
                                          SQLException {
      openStreams();

      resetProps();
      sysProps.put(FILE_PROP, fileStreamFile.getCanonicalPath());

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonEmpty(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkWrongFile() throws AssertException, IOException, 
                                               SQLException {
      openStreams();

      sysProps.put(FILE_PROP, 
                   new File(derbyHome+"foo", // erroneous path
                            makeStreamFilename("file")).getCanonicalPath());

      bootDerby();
      shutdownDerby();
      
      closeStreams();

      assertNonExisting(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertNonEmpty(errStreamFile);
   }


   private static void checkMethod() throws AssertException, IOException, 
                                            SQLException  {
      openStreams();

      resetProps();
      sysProps.put(METHOD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.getStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonExisting(fileStreamFile);
      assertNonEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkWrongMethod() throws AssertException, IOException, 
                                                 SQLException {
      openStreams();

      resetProps();
      sysProps.put(METHOD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.nonExistingGetStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonExisting(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertNonEmpty(errStreamFile);
   }


   private static void checkField() throws AssertException, IOException, 
                                           SQLException {
      openStreams();

      resetProps();
      sysProps.put(FIELD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.fieldStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonExisting(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertNonEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkWrongField() throws AssertException, IOException, 
                                                SQLException {
      openStreams();

      resetProps();
      sysProps.put(FIELD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.nonExistingFieldStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonExisting(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertNonEmpty(errStreamFile);
   }

   
   private static void checkFileOverMethod() throws AssertException, IOException, 
                                                    SQLException {
      openStreams();

      resetProps();
      sysProps.put(FILE_PROP, fileStreamFile.getCanonicalPath());
      sysProps.put(METHOD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.getStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonEmpty(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkFileOverField() throws AssertException, IOException, 
                                                   SQLException {
      openStreams();

      resetProps();
      sysProps.put(FILE_PROP, fileStreamFile.getCanonicalPath());
      sysProps.put(FIELD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.fieldStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonEmpty(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkFileOverMethodAndField() throws AssertException, 
                                                            IOException, 
                                                            SQLException {
      openStreams();

      resetProps();
      sysProps.put(FILE_PROP, fileStreamFile.getCanonicalPath());
      sysProps.put(METHOD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.getStream");
      sysProps.put(FIELD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.fieldStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonEmpty(fileStreamFile);
      assertEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   private static void checkMethodOverField() throws AssertException, IOException, 
                                                     SQLException {
      openStreams();

      resetProps();
      sysProps.put(METHOD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.getStream");
      sysProps.put(FIELD_PROP, 
                   "org.apache.derbyTesting.functionTests.tests.lang."+
                   "errorStream.fieldStream");

      bootDerby();
      shutdownDerby();

      closeStreams();

      assertNonExisting(fileStreamFile);
      assertNonEmpty(methodStreamFile);
      assertEmpty(fieldStreamFile);
      assertEmpty(errStreamFile);
   }


   public static void main(String[] args)
   {
      try {
         ij.getPropertyArg(args);
         sysProps  = System.getProperties();
         derbyHome = sysProps.getProperty("derby.system.home");

         System.out.println("Test errorStream starting");

         checkFile();
         checkWrongFile();

         checkMethod();
         checkWrongMethod();
	 
         checkField();
         checkWrongField();

         checkFileOverMethod();
         checkFileOverField();
         checkFileOverMethodAndField();
         
         checkMethodOverField();

         System.out.println("Test errorStream finished successfully");
      }
      catch (Exception e) {
         System.out.println("Test errorStream failed: " + e.getMessage());
         e.printStackTrace();
      };
   }
}

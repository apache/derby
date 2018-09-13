/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.SysinfoLocaleTest

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

/**
 * This test verifies that <code>sysinfo</code> correctly localizes its
 * messages according to the default locale and <code>derby.ui.locale</code>.
 */
public class SysinfoLocaleTest extends BaseTestCase {

    private static final String SYSINFO_CLASS_NAME = "org.apache.derby.tools.sysinfo";

    /** The encoding sysinfo should use for its output. */
    private static final String ENCODING = "UTF-8";

    /** Default locale to run this test under. */
    private final Locale defaultLocale;

    /** Properties used to start test */
    private final Properties _props;
  
    /**
     * Tells whether or not this test expects sysinfo's output to be localized
     * to German.
     */
    private final boolean localizedToGerman;

    /** Name of the test. */
    private final String name;

    /** The default locale before this test started. */
    private Locale savedLocale;

    /**
     * Create a test.
     *
     * @param defaultLocale the default locale for this test
     * @param german true if output is expected to be localized to German
     * @param info extra information to append to the test name (for debugging)
     * @param props Properties to use if starting with a module path
     */
    private SysinfoLocaleTest(Locale defaultLocale, boolean german,
                              String info, Properties props) {
        super("testSysinfoLocale");
        this.defaultLocale = defaultLocale;
        this.localizedToGerman = german;
        this._props = props;
        this.name = super.getName() + ":" + info;
    }

    /**
     * Returns the name of the test, which includes the default locale and
     * derby.ui.locale to aid debugging.
     *
     * @return name of the test
     */
    public String getName() {
        return name;
    }

    /**
     * Set up the test environment.
     */
    protected void setUp() {
        savedLocale = Locale.getDefault();
        Locale.setDefault(defaultLocale);
    }

    /**
     * Tear down the test environment.
     */
    protected void tearDown() throws Exception {
        Locale.setDefault(savedLocale);
        savedLocale = null;
        super.tearDown();
    }

    /**
     * Create a suite of tests.
     *
     * @return a test suite with different combinations of
     * <code>derby.ui.locale</code> and default locale
     */
    public static Test suite() {
        if (!Derby.hasTools()) {
            return new BaseTestSuite("empty: no tools support");
        }

        BaseTestSuite suite = new BaseTestSuite("SysinfoLocaleTest");

        // Create test combinations. Messages should be localized to the
        // locale specified by derby.ui.locale, if it's set. Otherwise, the
        // JVM's default locale should be used.
        suite.addTest(createTest(Locale.ITALY, null, false));
        suite.addTest(createTest(Locale.ITALY, "it_IT", false));
        suite.addTest(createTest(Locale.ITALY, "de_DE", true));
        suite.addTest(createTest(Locale.GERMANY, null, true));
        suite.addTest(createTest(Locale.GERMANY, "it_IT", false));
        suite.addTest(createTest(Locale.GERMANY, "de_DE", true));

        // This test creates a class loader. We don't want to grant that
        // permission to derbyTesting.jar since that means none of the tests
        // will notice if one of the product jars misses a privileged block
        // around the creation of a class loader.
        return SecurityManagerSetup.noSecurityManager(suite);
    }

    /**
     * Create a single test case.
     *
     * @param loc default locale for the test case
     * @param ui <code>derby.ui.locale</code> for the test case
     * @param german whether output is expected to be German
     */
    private static Test createTest(Locale loc, String ui, boolean german) {
        Properties prop = new Properties();
        if (ui != null) {
            prop.setProperty("derby.ui.locale", ui);
        }
        // always set the encoding so that we can reliably read the output
        prop.setProperty("derby.ui.codeset", ENCODING);

        String info = "defaultLocale=" + loc + ",uiLocale=" + ui;
        Test test = new SysinfoLocaleTest(loc, german, info, prop);
        return new SystemPropertyTestSetup(test, prop);
    }

    /**
     * Run a sysinfo class that is loaded in a separate class loader. A
     * separate class loader is required in order to force sysinfo to re-read
     * <code>derby.ui.locale</code> (happens when the class is loaded).
     */
    private static void runSysinfo() throws Exception {
        URL sysinfoURL = SecurityManagerSetup.getURL(SYSINFO_CLASS_NAME);
        URL emmaURL = getEmmaJar();
        URL[] urls = null;
        if(emmaURL != null) {
            urls = new URL[] { sysinfoURL, emmaURL };
        } else {
            urls = new URL[] { sysinfoURL };
        }

        // Create a new class loader that loads the Derby classes afresh.
        // Its parent (platformLoader) is a class loader that is able to
        // load the JDBC classes and other core classes needed by the Derby
        // classes.
        ClassLoader platformLoader = java.sql.Connection.class.getClassLoader();
        URLClassLoader loader = new URLClassLoader(urls, platformLoader);

        Class<?> copy = Class.forName(SYSINFO_CLASS_NAME, true, loader);
        Method main = copy.getMethod("main", new Class[] { String[].class });
        main.invoke(null, new Object[] { new String[0] });
    }

    /**
     * Run sysinfo and return its output as a string.
     *
     * @return output from sysinfo
     */
    private static String getSysinfoOutput() throws Exception {

        final PrintStream savedSystemOut = System.out;
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try {
            System.setOut(new PrintStream(bytes, true, ENCODING));
            runSysinfo();
        } finally {
            System.setOut(savedSystemOut);
        }

        return bytes.toString(ENCODING);
    }

    /**
     * Run sysinfo using the module path and return its output as a string.
     *
     * @return output from sysinfo
     */
    private String getSysinfoOutputWithModules() throws Exception
    {
        String modulePath = JVMInfo.getSystemModulePath();
        ArrayList<String> args = new ArrayList<String>();

        // setup locale
        String country = defaultLocale.getCountry();
        if ((country != null) && (country.length() > 0))
        {
            args.add("-Duser.country=" + country);
        }
        String language = defaultLocale.getLanguage();
        if ((language != null) && (language.length() > 0))
        {
            args.add("-Duser.language=" + language);
        }

        // add other properties: derby.ui.locale and derby.ui.codeset
        for (Object key : _props.keySet())
        {
            String keyName = (String) key;
            String value = _props.getProperty(keyName);
            args.add("-D" + key + "=" + value);
        }

        // add sysinfo entry point
        args.add("-m");
        args.add(ModuleUtil.TOOLS_MODULE_NAME + "/" + SYSINFO_CLASS_NAME);

        // now run the sysinfo command and collect its output
        final   String[]  command = new String[args.size()];
        args.toArray(command);

        Process sysinfoProcess = BaseTestCase.execJavaCmd
            (null, modulePath, command, null, true, true);
        String retval = readProcessOutput(sysinfoProcess);

        return retval;
    }
  
    /**
     * Some German strings that are expected to be in sysinfo's output when
     * localized to German.
     */
    private static final String[] GERMAN_STRINGS = {
        "BS-Name",
        "Java-Benutzername",
        "Derby-Informationen",
        "Informationen zum Gebietsschema",
    };

    /**
     * Some Italian strings that are expected to be in sysinfo's output when
     * localized to Italian.
     */
    private static final String[] ITALIAN_STRINGS = {
        "Nome SO",
        "Home utente Java",
        "Informazioni su Derby",
        "Informazioni sulle impostazioni nazionali",
    };

    /**
     * Checks that all the expected substrings are part of the output from
     * sysinfo. Fails if one or more of the substrings are not found.
     *
     * @param expectedSubstrings substrings in the expected locale
     * @param output the output from sysinfo
     */
    private void assertContains(String[] expectedSubstrings, String output) {
        for (int i = 0; i < expectedSubstrings.length; i++) {
            String s = expectedSubstrings[i];
            if (output.indexOf(s) == -1) {
                fail("Substring '" + s + "' not found in output: " + output);
            }
        }
    }

    /**
     * Test method which checks that the output from sysinfo is correctly
     * localized.
     */
    public void testSysinfoLocale() throws Exception {
        String output = JVMInfo.isModuleAware() ?
            getSysinfoOutputWithModules() : getSysinfoOutput();
        String[] expectedSubstrings =
                localizedToGerman ? GERMAN_STRINGS : ITALIAN_STRINGS;
        assertContains(expectedSubstrings, output);
    }
}

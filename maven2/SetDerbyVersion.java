/*

   Derby - Class SetDerbyVersion

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks the current Derby jars in the source tree directory, obtains the
 * Derby version from them, and replaces the value of the placeholder
 * version tags in the POM files.
 * <p>
 * After this method has been successfully run you should be ready to
 * generate the Maven 2 artifacts for Derby.
 * <p>
 * The main task of this class is to replace the version tags in the Maven
 * POM files. The can be done manually, but exact process would vary from
 * platform to platform. Also, running a search-and-replace could potentially
 * replace tags not supposed to be replaced. To make the Maven 2 artifact
 * publish process simpler, this class was written.
 */
//@NotThreadSafe
public class SetDerbyVersion {

    private static final String PROMPT_CONT_WARN =
            "Do you want to continue despite the warnings?";
    private static final String PROMPT_USE_SANE =
            "Do you want to generate artifacts with SANE jars?";
    private static final String JDBC_URL =
            "jdbc:derby:memory:testDB;create=true";
    private static final String REL_JAR_PATH = "../jars";
    private static final File SANE = new File(REL_JAR_PATH, "sane");
    private static final File INSANE = new File(REL_JAR_PATH, "insane");
    /** List of required jar files the Maven 2 Derby artifacts. */
    private static final String[] JARS = new String[] {
        "derbyshared.jar",
        "derby.jar",
        "derby.war",
        "derbynet.jar",
        "derbyclient.jar",
        "derbytools.jar",
        "derbyoptionaltools.jar",
        // Ignore derbyTesting.jar, not part of the Maven 2 artifacts.
        // "derbyTesting.jar",
        // Ignore derbyrun.jar, not part of the Maven 2 artifacts.
        //"derbyrun.jar",
        // The various locale files.
        "derbyLocale_cs.jar",
        "derbyLocale_de_DE.jar",
        "derbyLocale_es.jar",
        "derbyLocale_fr.jar",
        "derbyLocale_hu.jar",
        "derbyLocale_it.jar",
        "derbyLocale_ja_JP.jar",
        "derbyLocale_ko_KR.jar",
        "derbyLocale_pl.jar",
        "derbyLocale_pt_BR.jar",
        "derbyLocale_ru.jar",
        "derbyLocale_zh_CN.jar",
        "derbyLocale_zh_TW.jar",
    };

    /**
     * Displays a prompt and obtains a yes / no answer from standard in.
     *
     * @param prompt the prompt to display
     * @return {@code true} if the answer is yes, {@code false} if the answer
     *      is no.
     * @throws IOException if reading from standard in fails
     */
    private static boolean getYesNoInput(String prompt)
            throws IOException {
        // We don't care about keeping the objects around, this method will
        // only be used a few times.
        BufferedReader bIn = new BufferedReader(
                new InputStreamReader(System.in));
        while (true) {
            System.out.print(">> " + prompt + " (yes/no) ");
            String answer = bIn.readLine();
            if (answer == null) {
                // We don't know what do to here, so just fail.
                throw new EOFException("Input stream closed.");
            }
            if (answer.equals("yes")) {
                return true;
            } else if (answer.equals("no")) {
                return false;
            }
        }
    }

    /**
     * Set to {@code true} if a warning message is printed. Must be manually
     * reset if used to check for warnings in a part of the code.
     */
    private boolean warnings = false;
    /** The version string inserted into the POMs. */
    private String versionString = "ALPHA_VERSION";
    private File PREFIX;

    /**
     * Prints a warning message and sets the internal warning flag.
     *
     * @param msg the message to print
     */
    private void warn(String msg) {
        warnings = true;
        System.out.println("WARNING! " + msg);
    }

    private void info(String msg) {
        System.out.println(msg);
    }

    /**
     * Checks that all required jars are found in the jar directory.
     *
     * @return {@code true} if all required jars exist, {@code false} otherwise.
     */
    public boolean checkJars()
            throws Exception {
        if (!SANE.exists() && !INSANE.exists()) {
            warn("No jars exist. Produce a Derby release build.");
            return false;
        }
        if (SANE.exists() && INSANE.exists()) {
            warn("Both SANE and INSANE jars exist.");
            return false;
        }
        PREFIX = SANE.exists() ? SANE : INSANE;
        if (SANE.exists()) {
            warn("Only SANE jars exist. Normally INSANE jars are used for a " +
                    "release.");
            boolean answer = getYesNoInput(PROMPT_USE_SANE);
            if (!answer) {
                return false;
            }
        }
        URL[] URLS = new URL[JARS.length];
        for (int i=0; i < JARS.length; i++) {
            URLS[i] = new File(PREFIX, JARS[i]).toURI().toURL();
        }

        warnings = false; // Reuse the warnings flag.
        // Make sure the files are there.
        for (URL url : URLS) {
            File f = new File(url.toURI());
            info(String.format(
                    "Checking file: %-30s %,12d bytes",
                    f.getName(), f.length()));
            if (!f.exists()) {
                warn("Missing file: " + f.getCanonicalPath());
            } else if (f.length() == 0) {
                warn("Empty file: " + f.getCanonicalPath());
            }
        }
        info("");
        if (warnings) {
            // Fail here.
            warn("There are missing or empty jar files.");
            return false;
        }

        // The class loader used for the Derby jars.
        URLClassLoader cl = new URLClassLoader(URLS, null);

        // Fire up Derby to get the version string.
        Connection con = DriverManager.getConnection(JDBC_URL);
        DatabaseMetaData meta = con.getMetaData();

        // I.e.: 10.6.0.0 alpha - (882129M)
        String fullVersion = meta.getDatabaseProductVersion();
        String[] components = fullVersion.split(" - ");
        versionString = components[0].replaceAll(" ", "_");
        String srcRevision = components[1].replaceAll("\\(|\\)", "");
        info("Obtained product version string: " + fullVersion);
        info("(version=" + versionString + ", revision=" + srcRevision + ")");
        if (versionString.contains("beta")) {
            warn("This is a BETA build.");
        }
        if (versionString.contains("alpha")) {
            warn("This is an ALPHA build.");
        }
        if (srcRevision.endsWith("M")) {
            warn("The sources had been modified when the jars were built.");
            warnings = true;
        }
        if (warnings) {
            if (!getYesNoInput(PROMPT_CONT_WARN)) {
                return false;
            }
        }
        
        con.close();
        // Delete the derby.log file.
        new File("derby.log").delete();

        return true;
    }

    /**
     * Replaces the relevant version tags in the various POM files.
     *
     * @throws IOException if accessing a POM file fails
     */
    public boolean setPOMVersionTags()
            throws IOException {
        File curDir = new File(".");
        boolean gotWarnings = false;
        // We only descend one level, no need for a recursive method.
        for (File topLevel : curDir.listFiles()) {
            if (topLevel.getName().equals("pom.xml")) {
                gotWarnings |= setVersionTag(topLevel);
            }
            if (topLevel.isDirectory()) {
                for (File l1 : topLevel.listFiles()) {
                    if (l1.getName().equals("pom.xml")) {
                        gotWarnings |= setVersionTag(l1);
                        // There is only one POM in each sub-directory.
                        break;
                    }
                }
            }
        }

        // See if we ran into problems when replacing the version tags.
        info("");
        if (gotWarnings) {
            warn("There were errors replacing the POM version tags.");
        } else {
            info("POM version tags replacement succeeded.");
            info("It is recommended that you verify the POM diffs " +
                    "before running Maven.");
        }
        return (warnings == false);
    }

    public void printSanityNote()
            throws IOException {
        if (PREFIX == SANE) {
            info("");
            info("NOTE: Remember to change the <sanity> tag in the top-level");
            info("      POM, setting it to 'sane'.");
        }
    }

    /**
     * Verifies that the correct number of tags were replaced in the POM.
     *
     * @param replaceCount the number of tags replaced
     * @param pom the POM modified
     * @return {@code 0} if the check passed, a negative value if too few tags
     *      were replaced, and a positive value if too many tags were replaced.
     */
    private int checkResult(int replaceCount, File pom) {
        // The locales requires two replacements, due to the dependency.
        String parent = pom.getParent();
        if (parent.contains("derbyLocale")) {
            return (replaceCount - 2);
        // derbynet also requries two replacements (derby.jar dependency)
        } else if (parent.contains("net")) {
            return (replaceCount - 2);
        } else {
            return (replaceCount - 1);
        }
    }

    /**
     * Replaces all qualifying version tags in the specified POM.
     *
     * @param pom the POM to modify
     * @return {@code false} if warnings were produced when replacing,
     *      {@code true} if all seemed to go well.
     *
     * @throws IOException if reading or writing to the POM file fails
     */
    private boolean setVersionTag(File pom)
            throws IOException {
        // Clear internal warning flag.
        warnings = false;
        // Just read the whole file into memory.
        List<String> lines = readFile(pom);

        // Start writing the file back out, and search for tags to replace.
        BufferedWriter bOut = new BufferedWriter(new FileWriter(pom, false));
        int replaced = 0;
        boolean artifactIdOk = false;
        boolean groupIdOk = false;
        // Could have used XML, but keep it simple.
        // artifactId and groupId are used to qualify the version tag, as it can
        // be used in more places than those we want to replace.
        for (String line : lines) {
            // Look for tags for qualification.
            if (line.trim().startsWith("<artifactId>") &&
                    line.contains("derby")) {
                artifactIdOk = true;
            } else if (line.trim().startsWith("<groupId>") &&
                    line.contains("org.apache.derby")) {
                groupIdOk = true;
            }

            // Change line if a qualified version tag, echo otherwise.
            if (line.trim().startsWith("<version>") &&
                    artifactIdOk && groupIdOk) {
                // Replace tag.
                int whitespaceTag = line.indexOf(">");
                bOut.write(line.substring(0, whitespaceTag +1));
                bOut.write(versionString);
                int tagEnd = line.indexOf("<", whitespaceTag);
                bOut.write(line.substring(tagEnd));
                bOut.newLine();
                replaced++;
                artifactIdOk = groupIdOk = false;
            } else {
                bOut.write(line);
                bOut.newLine();
            }
        }
        try {
            bOut.flush();
            bOut.close();
        } catch (IOException ioe) {
            warn("Flushing/closing stream for " + pom.getCanonicalPath() +
                    " failed: " + ioe.getMessage());
        }
        int result = checkResult(replaced, pom);
        if (result == 0) {
            info("Replaced " + replaced + " version tag(s) in " +
                    pom.getParentFile().getName() + "/" + pom.getName());
        } else if (result > 0) {
            warn("Too many version tags (" + replaced + " > " +
                    (replaced - result) + ") replaced in " + pom.getPath());
        } else {
            warn("Too few version tags (" + replaced + " < " +
                    (replaced - result) + ") replaced in " + pom.getPath());
        }
        return warnings;
    }

    /**
     * Reads the contents of a text file.
     *
     * @param f the file to read
     * @return A list containing the lines of the file.
     * @throws IOException if reading the file fails
     */
    private List<String> readFile(File f)
            throws IOException {
        ArrayList<String> lines = new ArrayList<String>();
        BufferedReader bIn = new BufferedReader(new FileReader(f));
        String lineIn;
        try {
            while ((lineIn = bIn.readLine()) != null) {
                lines.add(lineIn);
            }
        } finally {
            try {
                bIn.close();
            } catch (IOException ioe) {
                // Just print a warning.
                warn("Failed to close input stream for " + f.getPath() + ": " +
                        ioe.getMessage());
            }
        }
        return lines;
    }

    /**
     * Quits the JVM if a failure is detected.
     *
     * @param success the return value to check
     */
    private static void ensureSuccess(boolean success) {
        if (!success) {
            System.out.println();
            System.out.println(
                    "!! The process failed or was aborted by the user.");
            System.out.println(
                    "   Read the above output and take corrective measures.");
            System.exit(1);
        }
    }

    /**
     * Checks the current Derby jars in the source tree directory, obtains the
     * Derby version from them, and replaces the value of the placeholder
     * version tags in the POM files.
     * <p>
     * After this method has been successfully run you should be ready to
     * generate the Maven 2 artifacts for Derby.
     *
     * @param args ignored
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args)
            throws Exception {
        SetDerbyVersion sdv = new SetDerbyVersion();
        ensureSuccess(sdv.checkJars());
        ensureSuccess(sdv.setPOMVersionTags());
        sdv.printSanityNote();
    }
}

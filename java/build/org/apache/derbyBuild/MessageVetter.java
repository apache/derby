/*

   Derby - Class org.apache.derbyBuild.MessageVetter

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

package org.apache.derbyBuild;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class that checks the message files for common problems.
 */
public class MessageVetter {

    /**
     * <p>
     * Check all the message translations in the specified directories for
     * common problems. Assume that all properties files in the directories
     * are message translations.
     * </p>
     *
     * <p>
     * If a problem is found, an error will be raised.
     * </p>
     *
     * @param args names of the directories to check
     */
    public static void main(String[] args) throws IOException {
        FileFilter filter = new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".properties");
            }
        };
        for (String directory : args) {
            for (File file : new File(directory).listFiles(filter)) {
                new MessageVetter(file).vet();
            }
        }
    }

    /**
     * A regular expression that matches a single-quote character that is
     * neither preceeded nor followed by another single-quote character. Used
     * by {@link #checkSingleQuotes(java.lang.String, java.lang.String)} to
     * verify that messages contain two single-quotes in order to produce a
     * single apostrophe (dictated by {@code java.text.MessageFormat}).
     */
    private static final Pattern LONE_QUOTE_PATTERN =
            Pattern.compile("^'[^']|[^']'[^']|[^']'$");

    /**
     * A regular expression that matches a single-quote character that have
     * no adjacent single-quote or curly brace character. Used by
     * {@link #checkSingleQuotes(java.lang.String, java.lang.String)} to
     * verify that all single-quotes are either correctly formatted apostrophes
     * or used for quoting curly braces, as required by
     * {@code java.text.MessageFormat}.
     */
    private static final Pattern LONE_QUOTE_ALLOWED_PATTERN =
            Pattern.compile("^'[^'{}]|[^'{}]'[^'{}]|[^'{}]'$");

    /**
     * A set of message identifiers in whose messages single-quotes may legally
     * appear with no adjacent single-quote character. This will be messages
     * where the single-quotes are needed to quote curly braces that should
     * appear literally in the message text.
     */
    private static final Set<String> LONE_QUOTE_ALLOWED = new HashSet<String>();
    static {
        // The IJ help text contains curly braces that need quoting.
        LONE_QUOTE_ALLOWED.add("IJ_HelpText");
        // Some of the DRDA usage messages contain the text {on|off}, which
        // needs quoting.
        LONE_QUOTE_ALLOWED.add("DRDA_Usage8.I");
        LONE_QUOTE_ALLOWED.add("DRDA_Usage11.I");
    }

    /** The message file to check. */
    private final File file;

    /** The properties found in the message file. */
    private final Properties properties;

    /**
     * Create a new {@code MessageVetter} instance.
     *
     * @param file the file with the messages to check
     * @throws IOException if the file cannot be loaded
     */
    private MessageVetter(File file) throws IOException {
        this.file = file;
        properties = new Properties();
        properties.load(new FileInputStream(file));
    }

    /**
     * Vet the messages in this file. An error will be raised if an
     * ill-formatted message is found.
     */
    private void vet() {
        Enumeration e = properties.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String message = properties.getProperty(key);
            vetMessage(key, message);
        }
    }

    /**
     * Vet a specific message. Raise an error if it is not well-formed.
     *
     * @param key the message identifier
     * @param message the message format specifier
     */
    private void vetMessage(String key, String message) {
        checkSingleQuotes(key, message);
        checkValidMessageFormat(key, message);
    }

    /**
     * Check that single-quote characters are doubled, as required by
     * {@code java.text.MessageFormat}. Raise an error otherwise.
     *
     * @param key the message identifier
     * @param message the message format specifier
     */
    private void checkSingleQuotes(String key, String message) {
        Pattern p;

        if (LONE_QUOTE_ALLOWED.contains(key)) {
            // In some messages we allow lone single-quote characters, but
            // only if they are used to quote curly braces. Use a regular
            // expression that finds all single-quotes that aren't adjacent to
            // another single-quote or a curly brace character.
            p = LONE_QUOTE_ALLOWED_PATTERN;
        } else {
            // Otherwise, we don't allow lone single-quote characters at all.
            p = LONE_QUOTE_PATTERN;
        }

        if (p.matcher(message).find()) {
            throw new AssertionError("Lone single-quote in message " + key +
                    " in " + file + ".\nThis is OK if it is used for quoting " +
                    "special characters in the message. If this is what the " +
                    "character is used for, add an exception in " +
                    getClass().getName() + ".LONE_QUOTE_ALLOWED.");
        }
    }

    /**
     * Check that a message format specifier is valid. Raise an error if it
     * is not.
     *
     * @param key the message identifier
     * @param message the message format specifier
     */
    private void checkValidMessageFormat(String key, String message) {
        try {
            // See if a MessageFormat instance can be produced based on this
            // message format specifier.
            new MessageFormat(message);
        } catch (Exception e) {
            AssertionError ae = new AssertionError(
                    "Message " + key + " in " + file + " isn't a valid " +
                    "java.text.MessageFormat pattern.");
            ae.initCause(e);
            throw ae;
        }
    }
}

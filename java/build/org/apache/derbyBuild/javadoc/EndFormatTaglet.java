/*

   Derby - Class org.apache.derbyBuild.javadoc.EndFormatTaglet

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derbyBuild.javadoc;

import com.sun.tools.doclets.Taglet;
import com.sun.javadoc.*;
import java.util.Map;

// Adapted from ToDoTaglet.java, Copyright  2002.

public class EndFormatTaglet implements Taglet {
    private String NAME = "end_format";
    private String ROWNAME = "end_format";

    /**
     * Returns the name of this taglet
     * @return NAME
     */
    public String getName() {
        return NAME;
    }

    /**
     * end_format not expected to be used in field documentation.
     * @return false
     */
    public boolean inField() {
        return false;
    }

    /**
     * end_format not expected to be used in constructor documentation.
     * @return false
     */
    public boolean inConstructor() {
        return false;
    }

    /**
     * end_format not expected to be used in method documentation.
     * @return false
     */
    public boolean inMethod() {
        return false;
    }

    /**
     * end_format can be used in overview documentation.
     * @return true
     */
    public boolean inOverview() {
        return true;
    }

    /**
     * end_format can be used in package documentation.
     * @return true
     */
    public boolean inPackage() {
        return true;
    }

    /**
     * end_format can be used in type documentation.
     * @return true
     */
    public boolean inType() {
        return true;
    }

    /**
     * end_format is not an inline tag.
     * @return false
     */
    public boolean isInlineTag() {
        return false;
    }

    /**
     * Register this Taglet.
     * @param tagletMap
     */
    public static void register(Map tagletMap) {
       EndFormatTaglet tag = new EndFormatTaglet();
       Taglet t = (Taglet) tagletMap.get(tag.getName());
       if (t != null) {
           tagletMap.remove(tag.getName());
       }
       tagletMap.put(tag.getName(), tag);
    }

    /**
     * No-op. Not currently used.
     * @param tag The tag to embed to the disk format table.
     */
    public String toString(Tag tag) {
        return "";
    }

    /**
     * No-op. Not currently used.
     * @param tags An array of tags to add to the disk format table.
     */
    public String toString(Tag[] tags) {
        if (tags.length == 0) {
            return null;
        }
        String result = "";
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) {
                result += "";
            }
            result += "";
        }
        return result + "\n";
    }
}


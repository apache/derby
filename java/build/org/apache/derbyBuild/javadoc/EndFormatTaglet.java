/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyBuild.javadoc
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derbyBuild.javadoc;

import com.sun.tools.doclets.Taglet;
import com.sun.javadoc.*;
import java.util.Map;

// Adapted from ToDoTaglet.java, Copyright  2002.

public class EndFormatTaglet implements Taglet {
    /**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;
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


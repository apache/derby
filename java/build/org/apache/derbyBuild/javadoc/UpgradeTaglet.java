/*

   Derby - Class org.apache.derbyBuild.javadoc.UpgradeTaglet

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

package org.apache.derbyBuild.javadoc;

import jdk.javadoc.doclet.Taglet;
import com.sun.source.doctree.DocTree;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.lang.model.element.Element;

public class UpgradeTaglet implements Taglet {
    private static final String NAME = "derby.upgrade";
    private static final String ROWNAME = "Upgrade";
    private static final EnumSet<Location> allowedSet = EnumSet.allOf(Location.class);

    /**
     * Returns the name of this taglet
     * @return NAME
     */
    public String getName() { return NAME; }

    @Override
    public Set<Taglet.Location> getAllowedLocations() { return allowedSet; }

    /**
     * upgrade is not an inline tag.
     * @return false
     */
    public boolean isInlineTag() {
        return false;
    }

    /**
     * Embed the contents of the upgrade tag as a row
     * in the disk format table.
     * @param tag The tag to embed to the disk format the table.
     */
    public String toString(DocTree tag) {
        return "<tr><td>" + ROWNAME + "</td>"
//IC see: https://issues.apache.org/jira/browse/DERBY-6974
          + "<td>" + DiskLayoutTaglet.getText(tag) + "</td></tr>\n";
    }

    /**
     * Embed multiple upgrade tags as cells in the disk format table.
     * @param tags An array of tags to add to the disk format table.
     * @param element the element to which the enclosing comment belongs
     */
    public String toString(List<? extends DocTree> tags, Element element)
    {
        if ((tags == null) || (tags.size() == 0)) {
            return null;
        }
        String result = "<tr><td>" + ROWNAME + "</td><td>";
        for (int i = 0; i < tags.size(); i++) {
            if (i > 0) {
                result += "";
            }
            result += "<tr><td>" + DiskLayoutTaglet.getText(tags.get(i)) + "</td></tr>";
        }
        return result + "\n";
    }
}


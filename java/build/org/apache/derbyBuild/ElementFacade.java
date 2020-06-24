/*  Derby - Class org.apache.derbyBuild.ElementFacade

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

import org.w3c.dom.*;
import java.util.*;

/**
 * A convenience wrapper around an XML Document Element. Provides some utility
 * methods for common operations on Element trees.
 */
public class ElementFacade {

    private Element root;

    /**
     * Construct a new ElementFacade from an Element.
     * @param r - the Element to wrap
     */
    public ElementFacade(Element r) {
        root = r;
    }

    /**
     * Construct a new ElementFacade from a Document (extract the top Element)
     * @param d document to get Element from
     * @throws java.lang.Exception
     */
    public ElementFacade(Document d) throws Exception {
        this(d.getDocumentElement());
    }

    /**
     * Lookup the Element subtree that starts with the specified tag. If more
     * than one, or no such tags exist an IllegalArgumentException is thrown.
     * @param tag to look up in wrapped tree
     * @return Element subtree rooted at the specified tag
     * @throws java.lang.Exception
     */
    public Element getElementByTagName(String tag) throws Exception {
        NodeList matchingTags = root.getElementsByTagName(tag);
        final int length = matchingTags.getLength();
        if (length != 1) {
            throw new IllegalArgumentException("Tag `" + tag + "' occurs " +
                    length + " times in Document.");
        }
        return (Element) matchingTags.item(0);
    }

    /**
     * Lookup the text (as String) identified by the specified tag. If more
     * than one, or no such tags exist an IllegalArgumentException is thrown.
     * @param tag to look up in wrapped tree
     * @return text corresponding to the specified tag
     * @throws java.lang.Exception
     */
    public String getTextByTagName(String tag) throws Exception {
        return getElementByTagName(tag).getFirstChild().getNodeValue();
    }

    /**
     * Produce a list of the texts specified by the
     * instances of tag in the wrapped tree. An empty list is retured if
     * there are no instances of tag in the tree.
     * @param tag to look up in wrapped tree
     * @return list of texts corresponding to the specified tag
     * @throws java.lang.Exception
     */
    public List<String> getTextListByTagName(String tag) throws Exception {
        NodeList matchingTags = root.getElementsByTagName(tag);
        final int length = matchingTags.getLength();
//IC see: https://issues.apache.org/jira/browse/DERBY-4893
        ArrayList<String> tagValues = new ArrayList<String>();
        for (int i = 0; i < length; ++i) {
            tagValues.add(matchingTags.item(i).getFirstChild().getNodeValue());
        }
        return tagValues;
    }
}

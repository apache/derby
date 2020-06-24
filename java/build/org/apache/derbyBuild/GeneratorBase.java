/*  Derby - Class org.apache.derbyBuild.GeneratorBase

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

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import org.w3c.dom.*;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

/**
 *
 */
public class GeneratorBase extends Task {
    /////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////////

    // header levels
    protected static final int BANNER_LEVEL = 1;
    protected static final int MAIN_SECTION_LEVEL = BANNER_LEVEL + 1;
    protected static final int ISSUE_DETAIL_LEVEL = MAIN_SECTION_LEVEL + 1;
    // headlines
    protected static final String DESCRIPTION_HEADLINE = "Description";
    protected static final String ISSUE_ID_HEADLINE = "Issue Id";

    // formatting tags
    private static final String ANCHOR = "a";
    protected static final String BODY = "body";
    private static final String BOLD = "b";
    private static final String BORDER = "border";
    private static final String COLUMN = "td";
    private static final String COLUMN_HEADER = "th";
    private static final String HORIZONTAL_LINE = "hr";
    protected static final String HTML = "html";
    private static final String DIVISION = "div";
    private static final String LIST = "ul";
    private static final String LIST_ELEMENT = "li";
    private static final String NAME = "name";
    protected static final String PARAGRAPH = "p";
    private static final String ROW = "tr";
    protected static final String SPAN = "span";
    private static final String TABLE = "table";

    // tags in summary xml
    private static final String SUM_PREVIOUS_RELEASE_ID = "previousReleaseID";
    private static final String SUM_RELEASE_ID = "releaseID";
    private static final String SUM_EXCLUDE_RELEASE_ID = "excludeReleaseID";

    // other html control
    protected static final int DEFAULT_TABLE_BORDER_WIDTH = 2;

    protected DocumentBuilder docBldr =
            DocumentBuilderFactory.newInstance().newDocumentBuilder();
    /////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////////
    // set on the command line or by ant
    // Summary file
    protected String summaryFileName;
    protected Document summaryDoc;
    protected ElementFacade summary;
    // Bug list file
    protected String bugListFileName;
    protected List bugList;

    // Output file
    private String outputFileName;
    protected Document outputDoc = docBldr.newDocument();
    protected File outputFile;

    // computed at run time
    protected String releaseID;
    protected String previousReleaseID;
    protected List<String> excludeReleaseIDList;
    protected final String branch;
    protected ArrayList<String> errors = new ArrayList<String>();

    // misc
    protected boolean _invokedByAnt = true;

    /**
     * Establishes state, including branch number.
     * @throws java.lang.Exception
     */
    public GeneratorBase() throws Exception {
        Properties r = new Properties();
        r.load(new FileInputStream("../ant/properties/release.properties"));
        int maint = Integer.parseInt(r.getProperty("maint"));
        branch = r.getProperty("eversion");
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  ANT Task BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////
    /**
     * Ant accessor to set the name of the summary file prepared by the
     * Release Manager
     * @param summaryFileName name of xml file to use for the summary
     * @throws Exception
     */
    public void setSummaryFileName(String summaryFileName) throws Exception {
        this.summaryFileName = summaryFileName;
        summaryDoc = docBldr.parse(new File(summaryFileName));
        summary = new ElementFacade(summaryDoc);
        previousReleaseID = summary.getTextByTagName(SUM_PREVIOUS_RELEASE_ID);

        excludeReleaseIDList =
                summary.getTextListByTagName(SUM_EXCLUDE_RELEASE_ID);
        System.out.println("Summary file (input): " + summaryFileName);
    }

    /**
     * Ant mutator to set the name of the JIRA-generated list of bugs
     * addressed by this release
     * @param bugListFileName name of an xml file from a Jira filter/query
     * @throws Exception
     */
    public void setBugListFileName(String bugListFileName) throws Exception {
        this.bugListFileName = bugListFileName;
    }

    /**
     * Ant mutator to set the name of the generated output file
     * @param outputFileName name of file to use as pamphlet (output)
     * @throws Exception
     */
    public void setOutputFileName(String outputFileName) throws Exception {
        this.outputFileName = outputFileName;
        outputFile = new File(outputFileName);
    }

    /**
     * Ant mutator to set the id of the release
     */
    public void setReleaseId(String releaseID) throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-4864
        this.releaseID = releaseID;
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////
    /**
     * Note that this release is a delta from the previous one.
     * @param parent
     * @throws java.lang.Exception
     */
    protected void buildDelta(Element parent)
            throws Exception {
        String deltaStatement =
//IC see: https://issues.apache.org/jira/browse/DERBY-4864
                "These notes describe the difference between Apache Derby release " +
                releaseID + " and the preceding release " +
                previousReleaseID + ".";

        addParagraph(parent, deltaStatement);
    }

    /**
     * Replace the known parameters with their corresponding text values.
     * @throws Exception
     */
    protected void replaceVariables()
            throws Exception {
        replaceTag(outputDoc, SUM_RELEASE_ID, releaseID);
        replaceTag(outputDoc, SUM_PREVIOUS_RELEASE_ID, previousReleaseID);
    }

    /**
     * Print the generated output document to the output file.
     * @throws Exception
     */
    protected void printOutput() throws Exception {
        Source source = new DOMSource(outputDoc);

        Result result = new StreamResult(outputFile);
        Transformer transformer =
                TransformerFactory.newInstance().newTransformer();

        transformer.transform(source, result);
    }

    /**
     * Print accumulated errors.
     * @throws Exception
     */
    protected void printErrors() throws Exception {
        if (errors.isEmpty()) {
            return;
        }

        println("The following other errors occurred:");

        for (Iterator i = errors.iterator(); i.hasNext();) {
            println("\n" + i.next());
        }
    }


    ////////////////////////////////////////////////////////
    //
    // HTML MINIONS
    //
    ////////////////////////////////////////////////////////
    /**
     * Create a section at the end of a parent element and link to it from a
     * table of contents.
     * @param parent in document tree
     * @param sectionLevel in document
     * @param toc table of content element
     * @param sectionName for section being created
     * @param tocEntry key into table of content
     * @return resulting Element
     * @throws Exception
     */
    public static Element createSection(Element parent, int sectionLevel,
            Element toc, String sectionName, String tocEntry)
            throws Exception {
        Document doc = parent.getOwnerDocument();
        Text textNode = doc.createTextNode(tocEntry);

        return createSection(parent, sectionLevel, toc, sectionName, textNode);
    }

    /**
     * Create a section at the end of a parent element and link to it from a
     * table of contents.
     * @param parent in document tree
     * @param sectionLevel in document
     * @param toc table of content element
     * @param sectionName for section being created
     * @param visibleText text to show
     * @return resulting Element
     * @throws Exception
     */
    public static Element createSection(Element parent, int sectionLevel,
            Element toc, String sectionName, Node visibleText)
            throws Exception {
        Document doc = parent.getOwnerDocument();
        Element link = createLocalLink(doc, sectionName, visibleText);

        addListItem(toc, link);

        return createHeader(parent, sectionLevel, sectionName);
    }

    /**
     * Create a header at the end of the parent node. Return the block created
     * to hold the text following this header.
     * @param parent
     * @param headerLevel
     * @param text
     * @return created header Element
     * @throws Exception
     */
    public static Element createHeader(Element parent, int headerLevel,
            String text) throws Exception {
        Document doc = parent.getOwnerDocument();
        Text textNode = doc.createTextNode(text);
        Element header = doc.createElement(makeHeaderTag(headerLevel));
        Element anchor = doc.createElement(ANCHOR);
        Element block = doc.createElement(DIVISION);
//IC see: https://issues.apache.org/jira/browse/DERBY-5181

        parent.appendChild(header);
        anchor.setAttribute(NAME, text);
        header.appendChild(anchor);
        header.appendChild(textNode);
        parent.appendChild(block);

        return block;
    }

    /**
     * Wraps the text content of the given node inside a div tag.
     *
     * @param node node currently containing the text
     * @return The new div-element which has been appended to {@code node}.
     * @throws DOMException 
     */
    private static Element wrapTextContentInDiv(Element node)
//IC see: https://issues.apache.org/jira/browse/DERBY-6044
            throws DOMException {
        Document doc = node.getOwnerDocument();
        Element div = doc.createElement(DIVISION);
        div.setTextContent(node.getTextContent());
        node.setTextContent("");
        node.appendChild(div);
        return div;
    }

    /**
     * Sets/overwrites the specified attribute.
     *
     * @param node target node
     * @param name attribute name
     * @param value attribute value
     * @throws DOMException 
     */
    private static void setAttribute(Element node, String name, String value)
            throws DOMException {
        Node attr = node.getOwnerDocument().createAttribute(name);
        attr.setNodeValue(value);

        node.getAttributes().setNamedItem(attr);
    }
    
    /**
     * Create an html text element.
     * @param doc
     * @param tag
     * @param text
     * @return created text Element
     * @throws Exception
     */
    public static Element createTextElement(Document doc, String tag,
            String text) throws Exception {
        Element retval = doc.createElement(tag);
        Text textNode = doc.createTextNode(text);

        retval.appendChild(textNode);

        return retval;
    }

    /**
     * Create a standard link to a local label.
     * @param doc
     * @param anchor
     * @param text
     * @return created link Element
     * @throws Exception
     */
    public static Element createLocalLink(Document doc, String anchor,
            String text) throws Exception {
        Text textNode = doc.createTextNode(text);

        return createLocalLink(doc, anchor, textNode);
    }

    /**
     * Create a standard link to a local label.
     * @param doc
     * @param anchor
     * @param visibleText
     * @return created link Element
     * @throws Exception
     */
    public static Element createLocalLink(Document doc, String anchor,
            Node visibleText) throws Exception {
        return createLink(doc, "#" + anchor, visibleText);
    }

    /**
     * Create a hotlink.
     * @param doc
     * @param label
     * @param text
     * @return created link Element
     * @throws Exception
     */
    public static Element createLink(Document doc, String label, String text)
            throws Exception {
        Text textNode = doc.createTextNode(text);

        return createLink(doc, label, textNode);
    }

    /**
     * Create a hotlink.
     * @param doc
     * @param label
     * @param visibleText
     * @return created anchor Element
     * @throws Exception
     */
    public static Element createLink(Document doc, String label,
            Node visibleText) throws Exception {
        Element hotlink = doc.createElement(ANCHOR);

        hotlink.setAttribute("href", label);
        hotlink.appendChild(visibleText);

        return hotlink;
    }

    /**
     * Insert a list at the end of the parent element.
     * @param parent
     * @return created list Element
     * @throws Exception
     */
    public static Element createList(Element parent)
            throws Exception {
        Document doc = parent.getOwnerDocument();
        Element list = doc.createElement(LIST);

        parent.appendChild(list);

        return list;
    }

    /**
     * Add an item with a bold name to the end of a list.
     * @param list
     * @param headline
     * @param text
     * @return created headline Element
     * @throws Exception
     */
    public static Element addHeadlinedItem(Element list, String headline,
            String text) throws Exception {
        Document doc = list.getOwnerDocument();
        Element itemElement = doc.createElement(LIST_ELEMENT);
        Element boldText = boldText(doc, headline);
        Text textNode = doc.createTextNode(" - " + text);

        list.appendChild(itemElement);
        itemElement.appendChild(boldText);
        itemElement.appendChild(textNode);

        return itemElement;
    }

    /**
     * Make some bold text.
     * @param doc
     * @param text
     * @return created bold Element
     * @throws Exception
     */
    public static Element boldText(Document doc, String text)
            throws Exception {
        Element bold = createTextElement(doc, BOLD, text);

        return bold;
    }

    /**
     * Add an item to the end of a list.
     * @param list
     * @param item
     * @return created item Element
     * @throws Exception
     */
    public static Element addListItem(Element list, Node item)
            throws Exception {
        Document doc = list.getOwnerDocument();
        Element itemElement = doc.createElement(LIST_ELEMENT);

        list.appendChild(itemElement);
        itemElement.appendChild(item);

        return itemElement;
    }

    /**
     * Retrieve the indented block inside a section
     * @param doc
     * @param sectionLevel
     * @param sectionName
     * @return indented block Element
     * @throws Exception
     */
    public static Element getSection(Document doc, int sectionLevel,
            String sectionName) throws Exception {
        String headerTag = makeHeaderTag(sectionLevel);
        Element root = doc.getDocumentElement();
        NodeList sectionList = root.getElementsByTagName(headerTag);
        int count = sectionList.getLength();

        for (int i = 0; i < count; i++) {
            Element section = (Element) sectionList.item(i);
            Element sectionAnchor = getFirstChild(section, ANCHOR);

            if (sectionName.equals(sectionAnchor.getAttribute(NAME))) {
                // the next item after the section header, is the indented block

                return (Element) section.getNextSibling();
            }
        }

        return null;
    }

    /**
     * Make the tag for a header, given its level
     * @param headerLevel
     * @return header tag at specified level
     */
    public static String makeHeaderTag(int headerLevel) {
        return "h" + Integer.toString(headerLevel);
    }

    /**
     * Add a paragraph to the end of a parent element.
     * @param parent
     * @param text
     * @return created paragraph Element
     * @throws Exception
     */
    public static Element addParagraph(Element parent, String text)
            throws Exception {
        Document doc = parent.getOwnerDocument();
        Text textNode = doc.createTextNode(text);
        Element paragraph = doc.createElement(PARAGRAPH);

        parent.appendChild(paragraph);
        paragraph.appendChild(textNode);

        return paragraph;
    }

    /**
     * Insert a table at the end of the parent element.
     * @param parent
     * @param borderWidth
     * @param columnHeadings
     * @return created table Element
     * @throws Exception
     */
    public static Element createTable(Element parent, int borderWidth,
            String[] columnHeadings) throws Exception {
        Document doc = parent.getOwnerDocument();
        Element table = doc.createElement(TABLE);
        Element headingRow = insertRow(table);

        parent.appendChild(table);
        table.setAttribute(BORDER, Integer.toString(borderWidth));

//IC see: https://issues.apache.org/jira/browse/DERBY-6044
        for (String headerText : columnHeadings) {
            Element headingColumn = insertColumnHeader(headingRow);
            headingColumn.setTextContent(headerText);
        }

        return table;
    }

    /**
     * Sets the width of the first column in the given table.
     *
     * @param table target table
     * @throws DOMException
     */
    protected void fixWidthOfFirstColumn(Element table)
            throws DOMException {
        NodeList headers = table.getElementsByTagName(COLUMN_HEADER);
        // Just fail if someone removes the th-elements.
        Element th = (Element)headers.item(0);
        Element div = wrapTextContentInDiv(th);
        setAttribute(div, "style", "width:110px;");
        th.appendChild(div);
    }
    
    /**
     * Insert a row at the end of a table
     * @param table
     * @return created row Element
     * @throws Exception
     */
    public static Element insertRow(Element table)
            throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-6044
        return insertTableElement(table, ROW);
    }

    /**
     * Insert a column at the end of a row
     * @param row
     * @return created column Element
     * @throws Exception
     */
    public static Element insertColumn(Element row)
            throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-6044
        return insertTableElement(row, COLUMN);
    }

    /**
     * Insert a header column at the end of the row.
     * @param row
     * @return Created column Element
     * @throws DOMException
     */
    public static Element insertColumnHeader(Element row)
            throws DOMException {
        return insertTableElement(row, COLUMN_HEADER);
    }

    /**
     * Inserts the specified element to the parent element.
     *
     * @param parent enclosing element, typically a table or a row
     * @param type type of the new element to be inserted, typically a
     *      column value or a row
     * @return The newly inserted element.
     * @throws DOMException if modifying the DOM fails
     */
    private static Element insertTableElement(Element parent, String type)
            throws DOMException {
        Document doc = parent.getOwnerDocument();
        Element newElement = doc.createElement(type);
        parent.appendChild(newElement);

        return newElement;
    }

    /**
     * Insert a horizontal line at the end of the parent element.
     * @param parent
     * @return created line Element
     * @throws Exception
     */
    public static Element insertLine(Element parent)
            throws Exception {
        Document doc = parent.getOwnerDocument();
        Element line = doc.createElement(HORIZONTAL_LINE);

        parent.appendChild(line);

        return line;
    }

    ////////////////////////////////////////////////////////
    //
    // XML MINIONS
    //
    ////////////////////////////////////////////////////////
    /**
     * Search the tree rooted at <code>node</code> for nodes tagged with
     * <code>childName</code>. Return the first such node, or throws an
     * exception if none is found.
     * @param node the node to search
     * @param childName name of child node to get
     * @return first child element
     * @throws Exception
     */
    public static Element getFirstChild(Element node, String childName)
            throws Exception {
        Element retval = getOptionalChild(node, childName);

        if (retval == null) {
            throw new BuildException("Could not find child element '" +
                    childName + "' in parent element '" +
                    node.getNodeName() + "'.");
        }
        return retval;
    }

    /**
     * Search the tree rooted at <code>node</code> for nodes tagged with
     * <code>childName</code>.
     * Return the index'th such node.
     * @param node parent
     * @param childName tag name of qualifying child nodes
     * @param index of child node to return
     * @return selected child node
     * @throws java.lang.Exception
     */
    public static Element getNextChild(Element node, String childName,
            int index) throws Exception {
        Element retval = (Element) node.getElementsByTagName(childName).item(index);

        if (retval == null) {
            throw new BuildException("Could not find child element '" +
                    childName + "' in parent element '" +
                    node.getNodeName() + "'.");
        }

        return retval;
    }

    /**
     * Search the tree rooted at <code>node</code> for nodes tagged with
     * <code>childName</code>. Return the first such node, or null if tag is
     * not found.
     * @param node root of subtree
     * @param childName type of child (tag) to search for
     * @return corresponding child if it exitsts, null otherwise.
     * @throws java.lang.Exception
     */
    public static Element getOptionalChild(Element node, String childName)
            throws Exception {
        return (Element) node.getElementsByTagName(childName).item(0);
    }

    /**
     * Squeeze the text out of an Element.
     * @param node with text child
     * @return String representation of node's first child
     * @throws java.lang.Exception
     */
    public static String squeezeText(Element node)
            throws Exception {
        return node.getFirstChild().getNodeValue();
    }

    /**
     * Clone all of the children of a source node and add them as children
     * of a target node.
     * @param source
     * @param target
     * @throws Exception
     */
    public static void cloneChildren(Node source, Node target)
            throws Exception {
        Document targetDoc = target.getOwnerDocument();
        NodeList sourceChildren = source.getChildNodes();
        int count = sourceChildren.getLength();

        for (int i = 0; i < count; i++) {
            Node sourceChild = sourceChildren.item(i);
            Node targetChild = targetDoc.importNode(sourceChild, true);
            target.appendChild(targetChild);
        }
    }

    /**
     * Replace all instances of the tag with the indicated text.
     * @param doc
     * @param tag
     * @param replacementText
     * @throws Exception
     */
    public static void replaceTag(Document doc, String tag,
            String replacementText) throws Exception {
        NodeList sourceChildren = doc.getElementsByTagName(tag);
        int count = sourceChildren.getLength();

        for (int i = 0; i < count; i++) {
            Node oldChild = sourceChildren.item(i);
            Node newChild = doc.createTextNode(replacementText);

            if (oldChild != null) {
                Node parent = oldChild.getParentNode();

                if (parent != null) {
                    parent.insertBefore(newChild, oldChild);
                }
            }
        }

        for (int i = count - 1; i > -1; i--) {
            Node oldChild = sourceChildren.item(i);

            if (oldChild != null) {
                Node parent = oldChild.getParentNode();

                if (parent != null) {
                    parent.removeChild(oldChild);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////
    //
    // EXCEPTION PROCESSING MINIONS
    //
    ////////////////////////////////////////////////////////
    /**
     * Format an error for later reporting.
     * @param text description
     * @param t execption that occured
     * @return formatted error with stack trace
     */
    public static String formatError(String text, Throwable t) {
        return (text + ": " + t.toString() + "\n" + stringifyStackTrace(t));
    }

    /**
     * Print a stack trace as a string.
     * @param t exception to dump stack for
     * @return String containing the stacl trace for the exception
     */
    public static String stringifyStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        t.printStackTrace(pw);
        pw.flush();
        sw.flush();

        return sw.toString();
    }

    ////////////////////////////////////////////////////////
    //
    // MISC MINIONS
    //
    ////////////////////////////////////////////////////////
    /**
     * Format a line of text.
     * @param text the line to format
     */
    protected void println(String text) {
        if (_invokedByAnt) {
            log(text, Project.MSG_WARN);
        } else {
            System.out.println(text);
        }
    }
}


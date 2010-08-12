package org.apache.derby.impl.tools.planexporter;

/**
 * This class is used by PlanExporter tool (DERBY-4587)
 * as a data structure to keep the values retrieved
 * after querying XPLAIN tables and few other properties
 * of a plan node in a query plan.
 */
class TreeNode{

    private String parentId = "";//PARENT_RS_ID
    private String id = "";//RS_ID
    private String nodeType = "";//OP_IDENTIFIER
    private String noOfOpens = "";//NO_OPENS
    private String inputRows = "";//INPUT_ROWS
    private String returnedRows = "";//RETURNED_ROWS
    private String visitedPages = "";//NO_VISITED_PAGES
    private String scanQualifiers = "";//SCAN_QUALIFIERS
    private String nextQualifiers = "";//NEXT_QUALIFIERS
    private String scannedObject = "";//SCAN_OBJECT_NAME
    private String scanType = "";//SCAN_TYPE
    private String sortType = "";//SORT_TYPE
    private String sorterOutput = "";//NO_OUTPUT_ROWS
    private int depth ;


    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }
    /**
     * @return the id
     */
    public String getId() {
        return id;
    }
    /**
     * @param parentId the parentId to set
     */
    public void setParent(String parentId) {
        this.parentId = parentId;
    }
    /**
     * @return the parentId
     */
    public String getParent() {
        return parentId;
    }
    /**
     * @param nodeType the nodeType to set
     */
    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }
    /**
     * @return the nodeType
     */
    public String getNodeType() {
        return nodeType;
    }
    /**
     * @param noOfOpens the noOfOpens to set
     */
    public void setNoOfOpens(String noOfOpens) {
        this.noOfOpens = noOfOpens;
    }
    /**
     * @return the noOfOpens
     */
    public String getNoOfOpens() {
        return noOfOpens;
    }
    /**
     * @param inputRows the inputRows to set
     */
    public void setInputRows(String inputRows) {
        this.inputRows = inputRows;
    }
    /**
     * @return the inputRows
     */
    public String getInputRows() {
        return inputRows;
    }
    /**
     * @param returnedRows the returnedRows to set
     */
    public void setReturnedRows(String returnedRows) {
        this.returnedRows = returnedRows;
    }
    /**
     * @return the returnedRows
     */
    public String getReturnedRows() {
        return returnedRows;
    }
    /**
     * @param visitedPages the visitedPages to set
     */
    public void setVisitedPages(String visitedPages) {
        this.visitedPages = visitedPages;
    }
    /**
     * @return the visitedPages
     */
    public String getVisitedPages() {
        return visitedPages;
    }
    /**
     * @param depth the depth to set
     */
    public void setDepth(int depth) {
        this.depth = depth;
    }
    /**
     * @return the depth
     */
    public int getDepth() {
        return depth;
    }
    /**
     * @param scanQualifiers the scanQualifiers to set
     */
    public void setScanQualifiers(String scanQualifiers) {
        this.scanQualifiers = scanQualifiers;
    }
    /**
     * @return the scanQualifiers
     */
    public String getScanQualifiers() {
        return scanQualifiers;
    }
    /**
     * @param nextQualifiers the nextQualifiers to set
     */
    public void setNextQualifiers(String nextQualifiers) {
        this.nextQualifiers = nextQualifiers;
    }
    /**
     * @return the nextQualifiers
     */
    public String getNextQualifiers() {
        return nextQualifiers;
    }
    /**
     * @param scannedObject the scannedObject to set
     */
    public void setScannedObject(String scannedObject) {
        this.scannedObject = scannedObject;
    }
    /**
     * @return the scannedObject
     */
    public String getScannedObject() {
        return scannedObject;
    }
    /**
     * @param scanType the scanType to set
     */
    public void setScanType(String scanType) {
        this.scanType = scanType;
    }
    /**
     * @return the scanType
     */
    public String getScanType() {
        return scanType;
    }
    /**
     * @param sortType the sortType to set
     */
    public void setSortType(String sortType) {
        this.sortType = sortType;
    }
    /**
     * @return the sortType
     */
    public String getSortType() {
        return sortType;
    }
    /**
     * @param sorterOutput the sorterOutput to set
     */
    public void setSorterOutput(String sorterOutput) {
        this.sorterOutput = sorterOutput;
    }
    /**
     * @return the sorterOutput
     */
    public String getSorterOutput() {
        return sorterOutput;
    }

    /**
     * @return XML fragment for this TreeNode object
     */
    public String toString(){
        String details = "<node ";
        details += getNodeType();
        details += getInputRows();
        details += getReturnedRows();
        details += getNoOfOpens();
        details += getVisitedPages();
        details += getScanQualifiers();
        details += getNextQualifiers();
        details += getScannedObject();
        details += getScanType();
        details += getSortType();
        details += getSorterOutput();

        return details+">\n";
    }
}

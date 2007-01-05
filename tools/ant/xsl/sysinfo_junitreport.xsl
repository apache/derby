<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<!--
 |   XSL Stylesheet for inserting a link to sysinfo output into junitreport
 |   output HTML files.
 |-->

<xsl:output method="xml" encoding="utf-8"/>

<!-- copy everything -->
<xsl:template match="*|@*">
  <xsl:copy>
    <xsl:apply-templates select="@*"/>
    <xsl:apply-templates select="node()"/>
  </xsl:copy>
</xsl:template>

<!-- insert sysinfo link into first h2 element. -->
<xsl:template match="h2[1]">
  <xsl:copy>
    <xsl:copy-of select="*"/><br/>
    <a href="sysinfo.txt" target="classFrame">sysinfo</a>
  </xsl:copy>
</xsl:template>

</xsl:stylesheet>

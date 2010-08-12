<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">

  <xsl:output method="html" indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd"/>

  <xsl:output method="html" indent="yes"/>

  <xsl:template match="/">
    <html>
      <head>
        <title>Derby Graphical Query Explainer</title>
      </head>
      <body>
        <IMG SRC="derby-logo.png" ALIGN="left"/>
        <center><H1>Apache Derby</H1></center>
        <center><H1>Graphical Query Explainer</H1></center>
        <center><H2>Executed Date &amp; Time: <font color="#4E9258"> <xsl:value-of select="//time"/> </font></H2></center>
        <center><H2>Query: <font color="#4E9258"> <xsl:value-of select="//statement"/> </font></H2></center>
        <center><H2>STMT_ID: <font color="#4E9258"> <xsl:value-of select="//stmt_id"/> </font></H2></center>
        <xsl:apply-templates select="plan/details/node"/>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="node">
    <ul>
        <br/>
        <h3><font face="verdana" color="#E56717"><xsl:value-of select="@name"/></font></h3>

        <table frame="border" rules="all">
        <xsl:if test="count(@input_rows)!=0">
            <tr>
            <xsl:apply-templates select="@input_rows"/>
            </tr>
        </xsl:if>
            <tr>
            <xsl:apply-templates select="@returned_rows"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@no_opens"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@visited_pages"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@scan_qualifiers"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@next_qualifiers"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@scanned_object"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@scan_type"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@sort_type"/>
            </tr>
            <tr>
            <xsl:apply-templates select="@sorter_output"/>
            </tr>
        </table>
        <xsl:apply-templates select="node"/>
    </ul>
  </xsl:template>

  <xsl:template match="node/@*">
    <th align="left">
        <xsl:value-of select="name()"/>
    </th>
    <td>
        <xsl:value-of select="."/>
    </td>
  </xsl:template>
</xsl:stylesheet>

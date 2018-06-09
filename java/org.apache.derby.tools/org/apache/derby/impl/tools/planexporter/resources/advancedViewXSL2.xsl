<!--

   Derby - Class advancedViewXSL2

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

 -->
<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">
 <!-- Designed & coded by C.S.Nirmal J. Fernando, of University of Moratuwa, Sri Lanka -->
  <xsl:output method="html" indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd"/>

  <xsl:strip-space elements="*"/>

  <xsl:template match="/">
    <html>
      <head>
        <title>Derby Graphical Query Explainer</title>
        <style type="text/css">
        H1,H2{text-align:center;}
        ul{list-style-type: none;}
        table.hide { display: none; }
        table,td,th
        {
            border:2px solid black;
            font-size:18px;
            position:relative;
            left:160px;
        }
        th
        {
            text-align:left;
            background-color:#999933;
            color:white;
        }
        td,tr{text-align:center;}
        span.expand, span.collapse
        {
            color:#3B5998;
            font-size:20px;
            position:relative;
            left:150px;
        }
        span.plus, span.minus
        {
            color:#000000;
            font-size:20px;
        }
        span.expand, span.collapse { cursor: pointer; }
        span.expand span.minus { display: none; }
        span.collapse span.plus { display: none }
        </style>
        <script type="text/javascript">
        <!--[CDATA[-->
        window.onload = function()
        {
          var ul = document.getElementById('main-ul');
          var childUls = ul.getElementsByTagName('table');
          for (var i = 0, l = childUls.length; i &lt; l; i++)
          {
            childUls[i].className = 'hide';
          }
        }

        function toggle(el)
        {
          do
          {
            var ul = el.nextSibling;
          }
          while (ul.tagName.toLowerCase() !== 'table');
          ul.className = ul.className === '' ? 'hide' : '';
          el.className = el.className === 'collapse' ? 'expand' : '';
        }

        function hide(el)
        {
            do
            {
                var ul = el.nextSibling;
            }
            while (ul.tagName.toLowerCase() !== 'table');
            ul.className = ul.className === '' ? 'hide' : '';
            el.className = el.className === 'expand' ? 'collapse' : '';
        }
       <!-- ]]-->
        </script>
      </head>
      <body>
        <H1>Apache Derby</H1>
        <H1>Graphical Query Explainer</H1>
        <H2>Executed Date &amp; Time: <font color="#4E9258"> <xsl:value-of select="//time"/> </font></H2>
        <H2>Query: <font color="#4E9258"> <xsl:value-of select="//statement"/> </font></H2>
        <H2>STMT_ID: <font color="#4E9258"> <xsl:value-of select="//stmt_id"/> </font></H2>
        <br></br>
        <br></br>
        <xsl:apply-templates/>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="plan">
     <ul id="main-ul">
      <xsl:apply-templates select="details/node">
        <xsl:with-param name="i" select="0"/>
    </xsl:apply-templates>
    </ul>
  </xsl:template>

  <xsl:template match="node">
  <xsl:param name="i"/>
    <li>
      <span class="collapse" onmouseover="toggle(this);" onmouseout="hide(this);">
         <xsl:if test="$i!=0">
            |_
        </xsl:if>
        <xsl:value-of select="@name"/>
        <br></br>
      </span>
      <table frame="border" rules="all">
        <xsl:if test="count(@input_rows)!=0">
            <tr>
            <xsl:apply-templates select="@input_rows"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@returned_rows)!=0">
            <tr>
            <xsl:apply-templates select="@returned_rows"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@no_opens)!=0">
            <tr>
            <xsl:apply-templates select="@no_opens"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@visited_pages)!=0">
            <tr>
            <xsl:apply-templates select="@visited_pages"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@scan_qualifiers)!=0">
            <tr>
            <xsl:apply-templates select="@scan_qualifiers"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@next_qualifiers)!=0">
            <tr>
            <xsl:apply-templates select="@next_qualifiers"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@scanned_object)!=0">
            <tr>
            <xsl:apply-templates select="@scanned_object"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@scan_type)!=0">
            <tr>
            <xsl:apply-templates select="@scan_type"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@sort_type)!=0">
            <tr>
            <xsl:apply-templates select="@sort_type"/>
            </tr>
        </xsl:if>
        <xsl:if test="count(@sorter_output)!=0">
            <tr>
            <xsl:apply-templates select="@sorter_output"/>
            </tr>
        </xsl:if>
        </table>
        <br></br>
        <ul>
          <xsl:apply-templates select="node"/>
        </ul>

    </li>
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

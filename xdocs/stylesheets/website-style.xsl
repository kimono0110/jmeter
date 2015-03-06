<?xml version="1.0" encoding="UTF-8"?>
<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor 
  license agreements. See the NOTICE file distributed with this work for additional 
  information regarding copyright ownership. The ASF licenses this file to 
  You under the Apache License, Version 2.0 (the "License"); you may not use 
  this file except in compliance with the License. You may obtain a copy of 
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required 
  by applicable law or agreed to in writing, software distributed under the 
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
  OF ANY KIND, either express or implied. See the License for the specific 
  language governing permissions and limitations under the License. -->
<!-- Content Stylesheet for "jmeter-site" -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="3.0"
>

  <!-- Defined parameters (overrideable) -->
  <xsl:param name="relative-path" select="'.'" />
  <xsl:param name="subdir" select="''" />
  <xsl:param name="imgdir" select="concat($relative-path, '/images')" />
  <xsl:param name="sshotdir" select="concat($imgdir, '/screenshots')" />
  <xsl:param name="cssdir" select="concat($relative-path, '/css')" />
  <xsl:param name="jakarta-site" select="'http://jakarta.apache.org'" />
  <xsl:param name="year" select="'2015'" />
  <xsl:param name="max-img-width" select="'600'" />

  <!-- Output method -->
  <xsl:output method="html" html-version="5.0" encoding="iso-8859-15"
    indent="no" doctype-system="about:legacy-compat" />

  <xsl:template match="document">
    <xsl:variable name="project" select="document('project.xml')/project" />
    <html lang="en">
      <head>
        <title>
          <xsl:value-of select="$project/title" />
          -
          <xsl:value-of select="properties/title" />
        </title>
        <xsl:for-each select="properties/author">
          <xsl:variable name="name">
            <xsl:value-of select="." />
          </xsl:variable>
          <xsl:variable name="email">
            <xsl:value-of select="@email" />
          </xsl:variable>
          <meta name="author" value="{$name}" />
          <meta name="email" value="{$email}" />
        </xsl:for-each>
        <!-- VIEWPORT -->
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link
          href='http://fonts.googleapis.com/css?family=Special+Elite:400normal'
          rel='stylesheet' type='text/css'
        ></link>
        <link rel="stylesheet" type="text/css"
          href="{concat($cssdir, '/new-style.css')}"
        ></link>
      </head>
      <body role="document">
        <a href="#content" class="hidden" >Main content</a>
        <div class="header">
          <xsl:comment>
            APACHE LOGO
          </xsl:comment>
          <div>
            <a href="http://www.apache.org">
              <img title="Apache Software Foundation" width="290"
                height="75" src="{$imgdir}/asf-logo.png" alt="Logo ASF" />
            </a>
          </div>
          <xsl:if test="$project/logo">
            <xsl:variable name="alt">
              <xsl:value-of select="$project/logo" />
            </xsl:variable>
            <xsl:variable name="home">
              <xsl:value-of select="$project/@href" />
            </xsl:variable>
            <xsl:variable name="src">
              <xsl:value-of
                select="concat($relative-path, $project/logo/@href)" />
            </xsl:variable>
            <xsl:comment>
              PROJECT LOGO
            </xsl:comment>
            <div>
              <a href="{$home}">
                <img src="{$src}" alt="{$alt}" />
              </a>
            </div>
          </xsl:if>
          <div>
            <div>
              <a href="https://twitter.com/share" class="twitter-share-button"
                data-text="Powerful Load Testing with Apache #JMeter"
                data-via="ApacheJMeter" data-lang="en-gb" data-size="large"
              >Tweet</a>
              <script><![CDATA[
            (function(d,s,id){
              var js,
                  fjs=d.getElementsByTagName(s)[0],
                  p=/^http:/.test(d.location)?'http':'https';
              if (!d.getElementById(id)) {
                  js=d.createElement(s);
                  js.id=id;
                  js.src=p+'://platform.twitter.com/widgets.js';
                  fjs.parentNode.insertBefore(js,fjs);
              }
            })(document, 'script', 'twitter-wjs');]]>
              </script>
            </div>
            <div>
              <a href="https://twitter.com/ApacheJMeter" class="twitter-follow-button"
                data-show-count="false" data-lang="en-gb" data-size="large"
              >Follow</a>
              <script><![CDATA[(function(d,s,id){
                var js,
                    fjs=d.getElementsByTagName(s)[0],
                    p=/^http:/.test(d.location)?'http':'https';
                if (!d.getElementById(id)) {
                    js=d.createElement(s);
                    js.id=id;
                    js.src=p+'://platform.twitter.com/widgets.js';
                    fjs.parentNode.insertBefore(js,fjs);
                }
            })(document, 'script', 'twitter-wjs');]]>
              </script>
            </div>
          </div>
          <div class="banner">
            <iframe src="http://www.apache.org/ads/button.html"
              style="border-width:0;" frameborder="0" scrolling="no"
            ></iframe>
            <div class="clear"></div>
          </div>
        </div>
        <div class="nav">
          <xsl:apply-templates select="$project/body/menu" />
        </div>
        <div class="main">
          <a name="content" />
          <xsl:call-template name="pagelinks" />
          <xsl:if test="@index">
            <xsl:call-template name="section-index" />
          </xsl:if>
          <xsl:apply-templates select="body/section"></xsl:apply-templates>
          <xsl:call-template name="pagelinks" />
        </div>
        <div class="footer">
          <div class="copyright">
            Copyright &amp;copy;
            1999 &amp;ndash;
            <xsl:value-of select="$year" />
            , Apache Software Foundation
          </div>
          <div class="trademarks">Apache, Apache JMeter, JMeter, the Apache
            feather, and the Apache JMeter logo are
            trademarks of the
            Apache Software Foundation.
          </div>
        </div>
      </body>
    </html>
  </xsl:template>

  <xsl:template name="pagelinks">
    <xsl:if test="@prev or @next">
      <ul class="pagelinks">
        <xsl:if test="@prev">
          <li>
            <a href="{@prev}">&lt; Prev</a>
          </li>
        </xsl:if>
        <li>
          <a href="{concat($relative-path, '/index.html')}">Index</a>
        </li>
        <xsl:if test="@next">
          <li>
            <a href="{@next}">Next &gt;</a>
          </li>
        </xsl:if>
      </ul>
    </xsl:if>
  </xsl:template>

  <xsl:template name="section-index">
    <ul class="section-index">
      <xsl:for-each select="body/section">
        <li>
          <a href="#{@anchor}">
            <xsl:value-of select="@name" />
          </a>
          <ul>
            <xsl:for-each select="component">
              <li>
                <a href="#{translate(normalize-space(@name), ' ', '_')}">
                  <xsl:value-of select="@name" />
                  <xsl:if test="@was">
                    (was:
                    <xsl:value-of select="@was" />
                    )
                  </xsl:if>
                </a>
              </li>
            </xsl:for-each>
          </ul>
        </li>
      </xsl:for-each>
    </ul>
  </xsl:template>

  <xsl:template name="image">
    <xsl:param name="srcdir" />
    <xsl:param name="image" />
    <xsl:param name="width" />
    <xsl:param name="height" />
    <xsl:variable name="name" select="concat($srcdir, '/', $image)" />
    <a href="{$name}"><img src="{$name}" width="{$width}" height="{$height}" /></a>
  </xsl:template>

  <!-- Process a menu for the navigation bar -->
  <xsl:template match="menu">
    <ul class="menu">
      <li>
        <div class="menu-title">
          <xsl:value-of select="@name" />
        </div>
        <ul>
          <xsl:apply-templates select="item" />
        </ul>
      </li>
    </ul>
  </xsl:template>

  <!-- Process a menu item for the navigation bar -->
  <xsl:template match="item">
    <xsl:variable name="href">
      <xsl:choose>
        <xsl:when test="starts-with(@href, 'http')">
          <xsl:value-of select="@href" />
        </xsl:when>
        <xsl:when test="starts-with(@href, '/site')">
          <xsl:value-of select="concat($jakarta-site, @href)" />
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="concat($relative-path, @href)" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <li>
      <a href="{$href}">
        <xsl:value-of select="@name" />
        <xsl:if test="@icon">
          <img src="{concat($imgdir, '/', @icon)}" />
        </xsl:if>
      </a>
    </li>
  </xsl:template>

  <xsl:template match="section">
    <div class="section">
      <h1>
        <xsl:value-of select="@name" />
        <xsl:if test="@anchor">
          <xsl:call-template name="sectionlink">
            <xsl:with-param name="anchor" select="@anchor" />
          </xsl:call-template>
        </xsl:if>
      </h1>
      <xsl:apply-templates />
    </div>
  </xsl:template>

  <xsl:template match="ch_section">
    <h2 class="ch_section">
      <a name="{.}" />
      <xsl:apply-templates />
    </h2>
  </xsl:template>

  <xsl:template match="ch_title">
    <h2 class="ch_title">
      <xsl:apply-templates />
    </h2>
  </xsl:template>

  <xsl:template match="ch_category">
    <h2 class="ch_category">
      <xsl:apply-templates />
    </h2>
  </xsl:template>

  <xsl:template match="subsection">
    <div class="subsection">
      <h2>
        <xsl:value-of select="@name" />
        <xsl:if test="@anchor">
          <xsl:call-template name="sectionlink">
            <xsl:with-param name="anchor" select="@anchor" />
          </xsl:call-template>
        </xsl:if>
      </h2>
      <xsl:apply-templates />
    </div>
  </xsl:template>

  <xsl:template match="source">
    <pre class="source">
      <xsl:apply-templates />
    </pre>
  </xsl:template>

  <xsl:template match="code">
    <span class="code">
      <xsl:apply-templates />
    </span>
  </xsl:template>

  <xsl:template match="description">
    <div class="description">
      <xsl:apply-templates />
    </div>
  </xsl:template>

  <xsl:template match="component">
    <div class="component">
      <h2>
        <xsl:value-of select="@name" />
        <xsl:if test="@was">
          <a name="{@was}">
            (was:
            <xsl:value-of select="@was" />
            )
          </a>
        </xsl:if>
        <xsl:if test="@name">
          <xsl:call-template name="sectionlink">
            <xsl:with-param name="anchor" select="@name" />
          </xsl:call-template>
        </xsl:if>
      </h2>
      <xsl:if test="@useinstead">
        <div class="deprecated">
          *** This element is deprecated. Use
          <a
            href="{concat($relative-path, '/usermanual/component_reference.html#', translate(normalize-space(@useinstead), ' ', '_'))}"
          >
            <xsl:value-of select="@useinstead" />
          </a>
          instead ***
        </div>
      </xsl:if>
      <xsl:if test="@screenshot != ''">
        <div class="screenshot">
          <xsl:call-template name="image">
            <xsl:with-param name="srcdir" select="$sshotdir" />
            <xsl:with-param name="image" select="@screenshot" />
            <xsl:with-param name="width" select="@width" />
            <xsl:with-param name="height" select="@height" />
          </xsl:call-template>
        </div>
      </xsl:if>
      <xsl:apply-templates />
      <div class="go-top">
        <a href="#">^</a>
      </div>
    </div>
  </xsl:template>

  <xsl:template name="sectionlink">
    <xsl:param name="anchor" />
    <a name="{translate(normalize-space($anchor), ' ', '_')}" />
    <a class="sectionlink" href="#{translate(normalize-space($anchor), ' ', '_')}"
      title="Link to here"
    >&amp;para;</a>
  </xsl:template>

  <xsl:template match="properties">
    <div class="properties">
      <h3>
        Parameters
        <xsl:if test="name(..) = 'component'">
          <xsl:call-template name="sectionlink">
            <xsl:with-param name="anchor"
              select="concat(translate(normalize-space(../@name), ' ', '_'), '_parms')" />
          </xsl:call-template>
        </xsl:if>
      </h3>
      <div class="property title">
        <div class="name title">Attribute</div>
        <div class="description title">Description</div>
        <div class="required title">Required</div>
      </div>
      <xsl:apply-templates />
    </div>
  </xsl:template>

  <xsl:template match="property">
    <div class="property">
      <div
        class="name req-{contains('yYtT', substring(normalize-space(@required), 1, 1))}"
      >
        <xsl:value-of select="@name" />
      </div>
      <div
        class="description req-{contains('yYtT', substring(normalize-space(@required), 1, 1))}"
      >
        <xsl:apply-templates />
      </div>
      <div
        class="required req-{contains('yYtT', substring(normalize-space(@required), 1, 1))}"
      >
        <xsl:choose>
          <xsl:when test="@required">
            <xsl:value-of select="@required" />
          </xsl:when>
          <xsl:otherwise>
            No
          </xsl:otherwise>
        </xsl:choose>
      </div>
    </div>
  </xsl:template>

  <xsl:template match="note">
    <div class="clear"></div>
    <div class="note">
      <xsl:apply-templates />
    </div>
    <div class="clear"></div>
  </xsl:template>

  <xsl:template name="complink">
    <xsl:param name="name" />
    <a
      href="{concat($relative-path, '/usermanual/component_reference.html#', translate(@name, ' ', '_'))}"
    >
      <xsl:value-of select="@name" />
    </a>
  </xsl:template>

  <xsl:template match="complink">
    <xsl:call-template name="complink">
      <xsl:with-param name="name" select="@name" />
    </xsl:call-template>
  </xsl:template>

  <xsl:template match="figure">
    <div class="figure">
      <xsl:call-template name="image">
        <xsl:with-param name="srcdir" select="$sshotdir" />
        <xsl:with-param name="image" select="@image" />
        <xsl:with-param name="width" select="@width" />
        <xsl:with-param name="height" select="@height" />
      </xsl:call-template>
      <figcaption>
        <xsl:apply-templates />
      </figcaption>
    </div>
  </xsl:template>

  <xsl:template match="bugzilla">
    <a href="http://bz.apache.org/bugzilla/show_bug.cgi?id={./text()}">
      Bug
      <xsl:value-of select="./text()" />
    </a>
  </xsl:template>

  <xsl:template match="bug">
    <a href="http://bz.apache.org/bugzilla/show_bug.cgi?id={./text()}">
      Bug
      <xsl:value-of select="./text()" />
    </a>
    -
  </xsl:template>

  <xsl:template match="links">
    <div class="links">
      <div class="title">See also:</div>
      <ul class="links">
        <xsl:for-each select="link|complink">
          <li>
            <xsl:choose>
              <xsl:when test="name(.) = 'link'">
                <a href="{@href}">
                  <xsl:apply-templates />
                </a>
              </xsl:when>
              <xsl:when test="name(.) = 'complink'">
                <xsl:call-template name="complink">
                  <xsl:with-param name="name" select="@name" />
                </xsl:call-template>
              </xsl:when>
            </xsl:choose>
          </li>
        </xsl:for-each>
      </ul>
    </div>
  </xsl:template>

  <xsl:template match="link">
    <li>
      <a href="{@href}">
        <xsl:apply-templates />
      </a>
    </li>
  </xsl:template>

  <xsl:template match="example">
    <div class="example">
      <div class="title">
        <xsl:value-of select="@title" />
        <xsl:if test="@anchor">
          <xsl:call-template name="sectionlink">
            <xsl:with-param name="anchor" select="@anchor" />
          </xsl:call-template>
        </xsl:if>
      </div>
      <xsl:apply-templates />
    </div>
  </xsl:template>

  <xsl:template match="table">
    <table>
      <xsl:apply-templates />
    </table>
  </xsl:template>

  <xsl:template
    match="h1|h2|h3|h4|h5|p|b|em|ul|ol|li|a|i|pre|br|tt|tr|th|td|dl|dt|dd|sup|span|u|strong|thead|tbody|form|select|option|input|font|center|img|body|style|div|hr"
  >
    <xsl:copy>
      <xsl:apply-templates select="@*|*|text()" />
    </xsl:copy>
  </xsl:template>


  <!-- Process everything else by just passing it through -->
  <xsl:template match="*">
    <div class="nostyle">
      <xsl:copy>
        <xsl:apply-templates select="@*|*|text()" />
      </xsl:copy>
    </div>
  </xsl:template>

  <xsl:template match="@*">
    <xsl:copy>
      <xsl:apply-templates select="@*|*|text()" />
    </xsl:copy>
  </xsl:template>

</xsl:stylesheet>

<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:Electricity_Infrastructure="http://win-amap-ext01:6080/arcgis/services/Electricity_Infrastructure/MapServer/WFSServer"
    xmlns:gml="http://www.opengis.net/gml">

<xsl:output method="text" encoding="utf-8" />

<xsl:template match="text()|@*"></xsl:template>

<xsl:param name="newline" select="'&#xA;'" />

<xsl:template match="/">
    <xsl:apply-templates/>
</xsl:template>

<xsl:template match="Electricity_Infrastructure:National_Major_Power_Stations">
    <xsl:value-of select="Electricity_Infrastructure:NAME"/>
    <xsl:text>,</xsl:text>
    <xsl:value-of select="Electricity_Infrastructure:SHAPE/gml:Point/gml:pos"/>
    <xsl:value-of select="$newline" />
</xsl:template>

</xsl:stylesheet>

<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:energy_ASEIS_Features="http://www.ga.gov.au/gis/services/energy/ASEIS_Features/MapServer/WFSServer"
    xmlns:gml="http://www.opengis.net/gml">

<xsl:output method="text" encoding="utf-8" />

<xsl:template match="text()|@*"></xsl:template>

<xsl:param name="newline" select="'&#xA;'" />

<xsl:template match="/">
    <xsl:apply-templates/>
</xsl:template>

<xsl:template match="energy_ASEIS_Features:Power_Stations">
    <xsl:value-of select="energy_ASEIS_Features:NAME"/>
    <xsl:text>,</xsl:text>
    <xsl:value-of select="energy_ASEIS_Features:SHAPE/gml:Point/gml:pos"/>
    <xsl:value-of select="$newline" />
</xsl:template>

</xsl:stylesheet>

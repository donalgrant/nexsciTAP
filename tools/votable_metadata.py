# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Column metadata carried inside Parquet files, the IVOA way ("VOParquet").

A Parquet file has column names and types but nowhere standard to say that a
column is in degrees or what it means.  The IVOA note "Parquet in the Virtual
Observatory" puts a VOTable with no rows in the file's key-value metadata:

    IVOA.VOTable-Parquet.version   "1.0"
    IVOA.VOTable-Parquet.content   <VOTABLE> ... <FIELD name= unit= ucd= ...>
                                   <DESCRIPTION>...</DESCRIPTION></FIELD> ...

make_parquet_catalog.py writes it and make_duckdb_catalog.py reads it into
TAP_SCHEMA, so a dataset's folder describes itself.  Tools that follow the
note (TOPCAT, astropy's "parquet.votable" format) read the same metadata.
"""

import xml.etree.ElementTree as ET

VERSION_KEY = 'IVOA.VOTable-Parquet.version'
CONTENT_KEY = 'IVOA.VOTable-Parquet.content'

NS = 'http://www.ivoa.net/xml/VOTable/v1.3'

# DuckDB type -> (VOTable datatype, arraysize)
VOTYPES = {'BOOLEAN': ('boolean', None), 'TINYINT': ('short', None), 'SMALLINT': ('short', None),
           'INTEGER': ('int', None), 'BIGINT': ('long', None), 'FLOAT': ('float', None),
           'DOUBLE': ('double', None)}

ATTRIBUTES = ('unit', 'ucd', 'utype')


def build(columns, table_description=''):
    """
    The VOTable text for `columns`: a list of (name, duckdb type, metadata),
    where metadata is a dict that may hold unit, ucd, utype and description.
    """

    votable = ET.Element('VOTABLE', {'version': '1.3', 'xmlns': NS})
    resource = ET.SubElement(votable, 'RESOURCE')
    table = ET.SubElement(resource, 'TABLE')
    if table_description:
        ET.SubElement(table, 'DESCRIPTION').text = table_description

    for name, duck_type, meta in columns:
        # Text gets the width of its longest value when the caller knows it:
        # astropy's reader of this metadata cannot combine a column declared
        # of any length ("*") with the text it then reads from the file.
        datatype, arraysize = VOTYPES.get(duck_type.upper(), ('char', str(meta.get('width') or '*')))
        attrs = {'name': name, 'datatype': datatype}
        if arraysize:
            attrs['arraysize'] = arraysize
        for key in ATTRIBUTES:
            if meta.get(key):
                attrs[key] = meta[key]
        field = ET.SubElement(table, 'FIELD', attrs)
        if meta.get('description'):
            ET.SubElement(field, 'DESCRIPTION').text = meta['description']

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(votable, encoding='unicode')


def parse(text):
    """
    (table description, {column name: {unit, ucd, utype, description}}) from
    the VOTable text.  Unreadable text gives ('', {}): metadata is a nicety,
    and a table is served without it rather than not at all.
    """

    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return '', {}

    def local(tag):
        return tag.rsplit('}', 1)[-1]

    table = next((e for e in root.iter() if local(e.tag) == 'TABLE'), None)
    if table is None:
        return '', {}

    description = ''
    columns = {}

    for child in table:
        if local(child.tag) == 'DESCRIPTION':
            description = (child.text or '').strip()
        elif local(child.tag) == 'FIELD' and child.get('name'):
            meta = {key: child.get(key) for key in ATTRIBUTES if child.get(key)}
            for sub in child:
                if local(sub.tag) == 'DESCRIPTION' and (sub.text or '').strip():
                    meta['description'] = sub.text.strip()
            columns[child.get('name')] = meta

    return description, columns

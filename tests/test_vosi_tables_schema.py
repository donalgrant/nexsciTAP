"""/tables must follow the VODataService schema.

TOPCAT's validator (taplint) reported every column of /tables as invalid:
dataType came before description, unit and ucd, and the columns carried
elements the schema does not have (arraysize, xtype, indexed, principal,
std, column_index).  The schema's order is name, description, unit, ucd,
utype, dataType, flag;  the array size and extended type are attributes of
dataType;  "indexed" and "principal" are flags;  "std" is an attribute.
"""
import io
import xml.etree.ElementTree as ET

from TAP import vositables

ORDER = ['name', 'description', 'unit', 'ucd', 'utype', 'dataType', 'flag']

# The order in which the service asks TAP_SCHEMA for a column's values.
NAMES = ['column_name', 'datatype', 'arraysize', 'xtype', 'description',
         'utype', 'unit', 'ucd', 'indexed', 'principal', 'std', 'column_index']

ROWS = [
    ('ra', 'double', None, None, 'Right ascension (J2000)', None, 'deg',
     'pos.eq.ra;meta.main', 1, 1, 0, 1),
    ('name', 'char', '*', None, 'Name, as in Smith & Jones <2020>', None,
     None, 'meta.id', 0, 0, 1, 2),
    ('when', 'char', '*', 'timestamp', None, None, None, None, 0, 0, 0, 3),
]


class Cursor:
    description = [(n, 'VARCHAR') for n in NAMES]
    arraysize = 0

    def __init__(self):
        self.rows = list(ROWS)

    def fetchmany(self):
        rows, self.rows = self.rows, []
        return rows


def _columns():
    writer = object.__new__(vositables.vosiTables)
    writer.debug = 0
    out = io.StringIO()
    writer.__writeOneTableResult__(Cursor(), out)
    return ET.fromstring('<table xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' + out.getvalue() + '</table>').findall('column')


def test_columns_follow_the_schema():
    columns = _columns()
    assert [c.findtext('name') for c in columns] == ['ra', 'name', 'when']
    for col in columns:
        tags = [c.tag for c in col]
        assert set(tags) <= set(ORDER), tags
        places = [ORDER.index(t) for t in tags]
        assert places == sorted(places), tags
        assert tags[0] == 'name' and tags.count('dataType') == 1
        assert set(col.attrib) <= {'std'}


def test_flags_attributes_and_escaping():
    ra, name, when = _columns()
    assert [f.text for f in ra.findall('flag')] == ['indexed', 'principal']
    assert ra.findtext('unit') == 'deg' and 'std' not in ra.attrib
    assert name.attrib == {'std': 'true'} and not name.findall('flag')
    assert name.findtext('description') == 'Name, as in Smith & Jones <2020>'
    assert name.find('dataType').attrib['arraysize'] == '*'
    assert when.find('dataType').attrib['extendedType'] == 'timestamp'
    assert when.find('description') is None and when.find('unit') is None

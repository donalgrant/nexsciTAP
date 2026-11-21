// The service's address is wherever this page was served from, so the same
// pages work under any name the service is given.
var TAP = location.origin + '/TAP';
document.querySelectorAll('.tapurl').forEach(function (e) { e.textContent = TAP; });
document.querySelectorAll('pre').forEach(function (e) {
    e.innerHTML = e.innerHTML.split('https://aiab.ipac.caltech.edu/TAP').join(TAP);
});
document.querySelectorAll('.copy').forEach(function (b) {
    b.addEventListener('click', function () {
        if (!navigator.clipboard) return;
        navigator.clipboard.writeText(TAP).then(function () {
            b.textContent = 'Copied'; setTimeout(function () { b.textContent = 'Copy'; }, 1500);
        });
    });
});

// One line of CSV, with quoted fields.
function csvRows(text) {
    var rows = [], row = [], field = '', quoted = false;
    for (var i = 0; i < text.length; i++) {
        var c = text[i];
        if (quoted) {
            if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
            else if (c === '"') quoted = false;
            else field += c;
        } else if (c === '"') quoted = true;
        else if (c === ',') { row.push(field); field = ''; }
        else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
        else if (c !== '\r') field += c;
    }
    if (field || row.length) { row.push(field); rows.push(row); }
    return rows;
}

// The catalogs, asked of the service itself, so the list is never out of date.
var list = document.getElementById('catalogs');
if (list) {
    var q = "SELECT table_name, description FROM TAP_SCHEMA.tables WHERE schema_name = 'main' ORDER BY table_name";
    fetch('/TAP/sync?LANG=ADQL&FORMAT=csv&QUERY=' + encodeURIComponent(q))
        .then(function (r) { if (!r.ok) throw new Error(r.status); return r.text(); })
        .then(function (text) {
            var rows = csvRows(text).slice(1).filter(function (r) { return r[0]; });
            if (!rows.length) throw new Error('empty');
            var body = list.querySelector('tbody');
            body.textContent = '';
            rows.forEach(function (r) {
                var tr = document.createElement('tr'), a = document.createElement('td'), b = document.createElement('td'),
                    code = document.createElement('code');
                code.textContent = r[0]; a.appendChild(code); b.textContent = r[1] || '';
                tr.appendChild(a); tr.appendChild(b); body.appendChild(tr);
            });
            document.getElementById('catalog-note').textContent =
                rows.length + ' tables, as the service lists them now.  Their columns and units are in ';
            var link = document.createElement('a');
            link.href = '/TAP/tables'; link.textContent = 'the TAP table list';
            document.getElementById('catalog-note').appendChild(link);
            document.getElementById('catalog-note').appendChild(document.createTextNode(', and in Firefly.'));
        })
        .catch(function () {
            list.querySelector('tbody').innerHTML = '<tr><td colspan="2">The list could not be read just now.  ' +
                'It is at <a href="/TAP/tables">/TAP/tables</a>.</td></tr>';
        });
}

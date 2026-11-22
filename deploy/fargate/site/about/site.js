// The service's address is wherever this page was served from, so the same
// pages work under any name the service is given.
var TAP = location.origin + '/TAP';
document.querySelectorAll('.tapurl').forEach(function (e) { e.textContent = TAP; });
document.querySelectorAll('pre').forEach(function (e) {
    e.innerHTML = e.innerHTML.split('https://aiab.ipac.caltech.edu/TAP').join(TAP);
});
// Copy the address.  Where the browser refuses the clipboard (an old browser,
// or a page that is not allowed it), the address is selected instead, so that
// the usual keys copy it; the button says which happened.
document.querySelectorAll('.copy').forEach(function (b) {
    b.addEventListener('click', function () {
        var said = function (t) { b.textContent = t; setTimeout(function () { b.textContent = 'Copy'; }, 2000); };
        var select = function () {
            var code = b.parentNode.querySelector('code'), r = document.createRange();
            r.selectNodeContents(code);
            var sel = getSelection(); sel.removeAllRanges(); sel.addRange(r);
            var ok = false;
            try { ok = document.execCommand('copy'); } catch (e) { /* not allowed */ }
            said(ok ? 'Copied' : 'Selected');
        };
        if (navigator.clipboard && navigator.clipboard.writeText)
            navigator.clipboard.writeText(TAP).then(function () { said('Copied'); }, select);
        else select();
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

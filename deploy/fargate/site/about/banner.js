// The administrator's notice to users, if one is set:  a strip across the top
// of the page.  The service's own pages include this script;  the proxy adds
// it to the portal's page.  The notice is /workspace/banner.json, kept level
// with what the control panel holds (deploy/ec2/banner.py), and absent when
// there is none.  Its message is shown as text, never as markup.
(function () {
    var ID = 'aiab-banner', shut = null;
    function show(n) {
        var old = document.getElementById(ID);
        if (!n || !n.message || n.message === shut) { if (old) old.remove(); return; }
        if (old && old.dataset.message === n.message) return;
        if (old) old.remove();
        var warn = n.level === 'warn', floating = location.pathname.indexOf('/about/') !== 0 && location.pathname !== '/';
        var bar = document.createElement('div');
        bar.id = ID;
        bar.dataset.message = n.message;
        bar.setAttribute('role', 'status');
        bar.style.cssText = 'font:15px/1.4 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;padding:9px 16px;' +
            'display:flex;gap:12px;align-items:flex-start;justify-content:center;box-sizing:border-box;' +
            (warn ? 'background:#fdecc8;color:#5c3b00;border-bottom:2px solid #d99a1c;'
                  : 'background:#e3eefb;color:#12345c;border-bottom:2px solid #5b8fd1;') +
            (floating ? 'position:fixed;top:0;left:0;right:0;z-index:100000;box-shadow:0 2px 8px rgba(0,0,0,.25);' : '');
        var text = document.createElement('span');
        text.textContent = n.message;
        var close = document.createElement('button');
        close.type = 'button';
        close.textContent = '×';
        close.setAttribute('aria-label', 'Dismiss this notice');
        close.style.cssText = 'border:0;background:none;color:inherit;font-size:20px;line-height:1;cursor:pointer;padding:0 4px;';
        close.onclick = function () { shut = n.message; bar.remove(); };
        bar.appendChild(text);
        bar.appendChild(close);
        document.body.insertBefore(bar, document.body.firstChild);
    }
    function look() {
        fetch('/workspace/banner.json', {cache: 'no-store'})
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(show, function () { /* no notice to be had:  leave the page as it is */ });
    }
    look();
    setInterval(look, 120000);
})();

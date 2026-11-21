"""The service's own pages (deploy/fargate/site):  everything they point at is there, and the proxy serves them."""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
FARGATE = os.path.join(HERE, '..', 'deploy', 'fargate')
SITE = os.path.join(FARGATE, 'site')
PAGES = ['index.html', 'about/examples.html']
# Paths the proxy hands to Firefly or to TAP, not files of the site.
ELSEWHERE = ('/firefly/', '/TAP/')


def read(*path):
    with open(os.path.join(*path), encoding='utf-8') as f:
        return f.read()


def test_every_local_link_leads_somewhere():
    for page in PAGES:
        html = read(SITE, page)
        for target in re.findall(r'(?:href|src)="([^"]+)"', html):
            if target.startswith('#') or target.startswith(ELSEWHERE):
                continue
            assert target.startswith('/'), f'{page}: {target} is not written from the root'
            path, _, anchor = target.partition('#')
            path = 'index.html' if path == '/' else path.lstrip('/')
            assert os.path.isfile(os.path.join(SITE, path)), f'{page}: {target} has no file'
            if anchor:
                assert f'id="{anchor}"' in read(SITE, path), f'{page}: {target} has no such place'


def test_anchors_within_a_page_exist():
    for page in PAGES:
        html = read(SITE, page)
        for anchor in re.findall(r'href="#([^"]+)"', html):
            assert f'id="{anchor}"' in html, f'{page}: #{anchor}'


def test_the_pages_name_no_host_but_the_one_the_script_replaces():
    """The address shown is filled in from where the page is served, so the pages hold only the default."""
    script = read(SITE, 'about', 'site.js')
    assert "location.origin + '/TAP'" in script
    for page in PAGES:
        hosts = set(re.findall(r'https?://([^/\s\'"<]+)', read(SITE, page)))
        assert hosts <= {'aiab.ipac.caltech.edu'}, hosts


def test_the_proxy_serves_the_pages():
    conf = read(FARGATE, 'nginx.conf.template')
    assert 'root /usr/share/nginx/aiab;' in conf
    assert 'location = / { try_files /index.html =404; }' in conf
    assert 'location /about/ { try_files $uri =404; }' in conf
    assert 'return 302 /firefly/' not in conf
    assert 'COPY site /usr/share/nginx/aiab' in read(FARGATE, 'Dockerfile.proxy')

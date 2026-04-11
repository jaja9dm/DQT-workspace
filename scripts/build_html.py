#!/usr/bin/env python3
"""
build_html.py
concept.md → docs/planning/concept.html 변환 스크립트
Claude Code 훅에서 자동 호출됨 (concept.md 수정 시)
"""

import re
import sys
from pathlib import Path

import markdown
from markdown.extensions.toc import TocExtension

ROOT = Path(__file__).parent.parent
MD_FILE = ROOT / "docs" / "planning" / "concept.md"
HTML_FILE = ROOT / "docs" / "planning" / "concept.html"


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #0e0e0c;
      --bg2: #161614;
      --bg3: #1e1e1c;
      --border: rgba(222,220,209,0.12);
      --text: #deddd1;
      --text-muted: #9c9a92;
      --accent: #5dcaa5;
      --accent2: #afa9ec;
      --accent3: #85b7eb;
      --yellow: #efa027;
      --red: #f0997b;
      --green: #97c459;
      --sidebar-w: 260px;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ scroll-behavior: smooth; }}

    body {{
      font-family: -apple-system, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      display: flex;
      min-height: 100vh;
      font-size: 15px;
      line-height: 1.75;
    }}

    /* ── 사이드바 ── */
    #sidebar {{
      width: var(--sidebar-w);
      min-width: var(--sidebar-w);
      background: var(--bg2);
      border-right: 1px solid var(--border);
      position: sticky;
      top: 0;
      height: 100vh;
      overflow-y: auto;
      padding: 24px 0;
      flex-shrink: 0;
    }}
    #sidebar .logo {{
      padding: 0 20px 20px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 16px;
    }}
    #sidebar .logo span {{
      display: block;
      font-size: 13px;
      font-weight: 700;
      color: var(--accent);
      letter-spacing: .06em;
      text-transform: uppercase;
    }}
    #sidebar .logo small {{
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
      display: block;
    }}
    #sidebar nav a {{
      display: block;
      padding: 5px 20px;
      font-size: 13px;
      color: var(--text-muted);
      text-decoration: none;
      border-left: 2px solid transparent;
      transition: all .15s;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    #sidebar nav a:hover {{
      color: var(--text);
      border-left-color: var(--accent);
      background: rgba(93,202,165,.06);
    }}
    #sidebar nav a.h2 {{ padding-left: 20px; font-weight: 600; color: var(--text); }}
    #sidebar nav a.h3 {{ padding-left: 32px; font-size: 12px; }}

    /* ── 메인 콘텐츠 ── */
    #content {{
      flex: 1;
      max-width: 860px;
      padding: 48px 56px;
      overflow-x: hidden;
    }}

    /* ── 헤딩 ── */
    h1 {{
      font-size: 26px;
      font-weight: 700;
      color: var(--text);
      margin-bottom: 8px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
    }}
    h2 {{
      font-size: 18px;
      font-weight: 700;
      color: var(--text);
      margin: 40px 0 12px;
      padding-top: 8px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 8px;
    }}
    h3 {{
      font-size: 15px;
      font-weight: 700;
      color: var(--accent);
      margin: 28px 0 10px;
    }}
    h4 {{
      font-size: 14px;
      font-weight: 600;
      color: var(--text-muted);
      margin: 20px 0 8px;
      text-transform: uppercase;
      letter-spacing: .05em;
    }}

    /* ── 단락 ── */
    p {{ margin-bottom: 12px; }}

    /* ── 인용구 ── */
    blockquote {{
      border-left: 3px solid var(--accent2);
      padding: 10px 16px;
      background: rgba(175,169,236,.07);
      border-radius: 0 6px 6px 0;
      color: var(--text-muted);
      font-size: 14px;
      margin: 16px 0;
    }}

    /* ── 리스트 ── */
    ul, ol {{
      padding-left: 22px;
      margin-bottom: 12px;
    }}
    li {{ margin-bottom: 4px; }}

    /* ── 인라인 코드 ── */
    code {{
      background: var(--bg3);
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 1px 6px;
      font-family: "JetBrains Mono", "Fira Code", monospace;
      font-size: 13px;
      color: var(--accent);
    }}

    /* ── 코드 블록 ── */
    pre {{
      background: var(--bg3);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px 20px;
      overflow-x: auto;
      margin: 16px 0;
    }}
    pre code {{
      background: none;
      border: none;
      padding: 0;
      font-size: 13px;
      color: var(--text);
      line-height: 1.6;
    }}

    /* ── 테이블 ── */
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 16px 0;
      font-size: 14px;
    }}
    th {{
      background: var(--bg3);
      color: var(--text-muted);
      font-weight: 600;
      text-align: left;
      padding: 8px 12px;
      border-bottom: 1px solid var(--border);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .04em;
    }}
    td {{
      padding: 8px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: rgba(255,255,255,.02); }}

    /* ── 수평선 ── */
    hr {{
      border: none;
      border-top: 1px solid var(--border);
      margin: 32px 0;
    }}

    /* ── 강조 ── */
    strong {{ color: var(--text); font-weight: 700; }}
    em {{ color: var(--text-muted); }}

    /* ── 이미지 (SVG) ── */
    img, svg {{
      max-width: 100%;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: var(--bg3);
      display: block;
      margin: 16px auto;
    }}

    /* ── 배지 (버전/상태 테이블) ── */
    td:last-child code {{
      color: var(--yellow);
    }}

    /* ── 푸터 ── */
    #footer {{
      margin-top: 64px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
      font-size: 12px;
      color: var(--text-muted);
      display: flex;
      justify-content: space-between;
    }}

    /* ── 반응형 ── */
    @media (max-width: 768px) {{
      #sidebar {{ display: none; }}
      #content {{ padding: 24px 20px; }}
    }}
  </style>
</head>
<body>

<aside id="sidebar">
  <div class="logo">
    <span>DQT-workspace</span>
    <small>Dean Quant Trading</small>
  </div>
  <nav id="toc-nav">
{toc_nav}
  </nav>
</aside>

<main id="content">
  {body}
  <div id="footer">
    <span>DQT-workspace 기획서</span>
    <span>최종 업데이트: {updated}</span>
  </div>
</main>

<script>
  // 스크롤 시 현재 섹션 사이드바 하이라이트
  const links = document.querySelectorAll('#toc-nav a');
  const headings = Array.from(document.querySelectorAll('h2, h3'));
  window.addEventListener('scroll', () => {{
    const y = window.scrollY + 80;
    let current = '';
    headings.forEach(h => {{ if (h.offsetTop <= y) current = h.id; }});
    links.forEach(a => {{
      a.style.color = '';
      a.style.borderLeftColor = '';
      if (a.getAttribute('href') === '#' + current) {{
        a.style.color = 'var(--accent)';
        a.style.borderLeftColor = 'var(--accent)';
      }}
    }});
  }});
</script>

</body>
</html>
"""


def slugify(text: str, sep: str = '-') -> str:
    text = re.sub(r'[^\w\s가-힣-]', '', text)
    text = re.sub(r'\s+', sep, text.strip())
    return text.lower()


def build_toc_nav(md_text: str) -> str:
    lines = []
    for line in md_text.splitlines():
        m2 = re.match(r'^## (.+)', line)
        m3 = re.match(r'^### (.+)', line)
        if m2:
            title = m2.group(1).strip()
            slug = slugify(title)
            lines.append(f'    <a class="h2" href="#{slug}">{title}</a>')
        elif m3:
            title = m3.group(1).strip()
            slug = slugify(title)
            lines.append(f'    <a class="h3" href="#{slug}">{title}</a>')
    return '\n'.join(lines)


def convert(md_path: Path, html_path: Path) -> None:
    md_text = md_path.read_text(encoding='utf-8')

    # python-markdown 변환
    md = markdown.Markdown(
        extensions=[
            'tables',
            'fenced_code',
            'attr_list',
            'md_in_html',
            TocExtension(slugify=slugify, permalink=False),
        ]
    )
    body_html = md.convert(md_text)

    # 타이틀 추출 (첫 번째 h1)
    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', body_html)
    title = re.sub(r'<[^>]+>', '', title_match.group(1)) if title_match else 'DQT 기획서'

    # 사이드바 TOC 생성
    toc_nav = build_toc_nav(md_text)

    from datetime import date
    updated = date.today().strftime('%Y-%m-%d')

    html = HTML_TEMPLATE.format(
        title=title,
        toc_nav=toc_nav,
        body=body_html,
        updated=updated,
    )

    html_path.write_text(html, encoding='utf-8')
    print(f'[build_html] {md_path.name} → {html_path.name}')


if __name__ == '__main__':
    # 인자로 특정 md 파일 지정 가능: python build_html.py path/to/file.md
    if len(sys.argv) > 1:
        md_path = Path(sys.argv[1]).resolve()
        html_path = md_path.with_suffix('.html')
    else:
        md_path = MD_FILE
        html_path = HTML_FILE

    if not md_path.exists():
        print(f'[build_html] ERROR: {md_path} not found', file=sys.stderr)
        sys.exit(1)

    convert(md_path, html_path)

"""
domain/email_content.py — Transformación de contenido, sin I/O.

md_to_html() y build_html_email() son funciones puras: no leen configuración
ni tocan la base de datos. El estilo (colores, fuente, firma, nombre del
remitente) se les pasa ya resuelto — quien arma ese dict es
services/settings_service.py.
"""
import re


def md_to_html(text: str) -> str:
    """Convierte Markdown liviano (tablas, encabezados, énfasis, links,
    listas, párrafos) a HTML apto para email. Idéntico al del proyecto
    original — es la función que le da su formato a cada comunicación."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def render_table(block: str) -> str:
        lines = [l.strip() for l in block.strip().split("\n") if l.strip()]
        if len(lines) < 2:
            return block
        header_line = lines[0]
        sep_line = lines[1] if len(lines) > 1 else ""
        data_lines = lines[2:] if len(lines) > 2 else []

        if not re.match(r"^[\|\s\-:]+$", sep_line):
            return block

        def parse_row(line):
            line = line.strip().strip("|")
            return [c.strip() for c in line.split("|")]

        sep_cells = parse_row(sep_line)
        aligns = []
        for cell in sep_cells:
            cell = cell.strip()
            if cell.startswith(":") and cell.endswith(":"):
                aligns.append("center")
            elif cell.endswith(":"):
                aligns.append("right")
            else:
                aligns.append("left")

        th_cells = parse_row(header_line)
        td_style_base = "padding:9px 14px;border-bottom:1px solid #e8e8e8;font-size:14px;line-height:1.5;"
        th_style_base = (
            "padding:10px 14px;font-size:12px;font-weight:600;letter-spacing:.05em;"
            "text-transform:uppercase;border-bottom:2px solid PRIMARY_COLOR;background:#f9fafb;"
        )

        ths = "".join(
            f'<th style="{th_style_base}text-align:{aligns[i] if i < len(aligns) else "left"}">{c}</th>'
            for i, c in enumerate(th_cells)
        )

        rows_html = ""
        for ri, row_line in enumerate(data_lines):
            cells = parse_row(row_line)
            bg = "#ffffff" if ri % 2 == 0 else "#f7f7f7"
            tds = "".join(
                f'<td style="{td_style_base}text-align:{aligns[i] if i < len(aligns) else "left"};background:{bg}">{c}</td>'
                for i, c in enumerate(cells)
            )
            rows_html += f"<tr>{tds}</tr>"

        return (
            '<table role="presentation" cellpadding="0" cellspacing="0" border="0" '
            'style="width:100%;border-collapse:collapse;margin:16px 0;border:1px solid #e8e8e8;border-radius:6px;overflow:hidden">'
            f"<thead><tr>{ths}</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table>"
        )

    def process_tables(txt: str) -> str:
        output_parts = []
        lines = txt.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]
            if "|" in line:
                table_lines = []
                while i < len(lines) and ("|" in lines[i] or lines[i].strip() == ""):
                    if "|" in lines[i]:
                        table_lines.append(lines[i])
                    else:
                        break
                    i += 1
                if len(table_lines) >= 2:
                    output_parts.append(render_table("\n".join(table_lines)))
                else:
                    output_parts.extend(table_lines)
            else:
                output_parts.append(line)
                i += 1
        return "\n".join(output_parts)

    text = process_tables(text)

    text = re.sub(r"^### (.+)$", r'<h3 style="margin:16px 0 8px;font-size:1.1em">\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r"^## (.+)$", r'<h2 style="margin:20px 0 10px;font-size:1.3em">\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r"^# (.+)$", r'<h1 style="margin:24px 0 12px;font-size:1.5em">\1</h1>', text, flags=re.MULTILINE)

    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"<strong><em>\1</em></strong>", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    text = re.sub(r"__(.+?)__", r"<strong>\1</strong>", text)
    text = re.sub(r"_(.+?)_", r"<em>\1</em>", text)

    text = re.sub(r"\[(.+?)\]\((.+?)\)", r'<a href="\2" style="color:LINKCOLOR;text-decoration:underline">\1</a>', text)

    text = re.sub(r"^\s*[-*_]{3,}\s*$", '<hr style="border:none;border-top:1px solid #e4e4e4;margin:20px 0"/>', text, flags=re.MULTILINE)

    lines = text.split("\n")
    result, in_ul, in_ol = [], False, False
    for line in lines:
        ul_match = re.match(r"^[-*•] (.+)", line)
        ol_match = re.match(r"^\d+\. (.+)", line)
        if ul_match:
            if in_ol:
                result.append("</ol>")
                in_ol = False
            if not in_ul:
                result.append('<ul style="margin:10px 0;padding-left:24px">')
                in_ul = True
            result.append(f'<li style="margin:5px 0">{ul_match.group(1)}</li>')
        elif ol_match:
            if in_ul:
                result.append("</ul>")
                in_ul = False
            if not in_ol:
                result.append('<ol style="margin:10px 0;padding-left:24px">')
                in_ol = True
            result.append(f'<li style="margin:5px 0">{ol_match.group(1)}</li>')
        else:
            if in_ul:
                result.append("</ul>")
                in_ul = False
            if in_ol:
                result.append("</ol>")
                in_ol = False
            result.append(line)
    if in_ul:
        result.append("</ul>")
    if in_ol:
        result.append("</ol>")
    text = "\n".join(result)

    paragraphs = re.split(r"\n\n+", text)
    wrapped = []
    for p in paragraphs:
        p = p.strip()
        if not p:
            continue
        if p.startswith("<h") or p.startswith("<ul") or p.startswith("<ol") or p.startswith("<li") or p.startswith("<table") or p.startswith("<hr"):
            wrapped.append(p)
        else:
            wrapped.append(f'<p style="margin:0 0 14px;line-height:1.7">{p.replace(chr(10), "<br>")}</p>')
    return "\n".join(wrapped)


def build_html_email(body_text: str, style_cfg: dict = None) -> str:
    """Envuelve el HTML del cuerpo en la plantilla visual de la comunicación
    (header, firma, footer). `style_cfg` debe traer sender_name y
    signature_html ya resueltos por quien llama."""
    s = style_cfg or {}
    primary = s.get("primary_color", "#7ec850")
    bg = s.get("bg_color", "#ffffff")
    text_col = s.get("text_color", "#1a1a1a")
    font = s.get("font_family", "Georgia,'Times New Roman',serif")
    fsize = s.get("font_size", "16px")
    link_col = s.get("link_color", "#2563eb")
    header_bg = s.get("header_bg", "#0c0f0a")
    header_fc = s.get("header_color", "#7ec850")
    sname = s.get("sender_name", "")
    sig_html = s.get("signature_html", "")
    body_html = md_to_html(body_text).replace("LINKCOLOR", link_col).replace("PRIMARY_COLOR", primary)

    sig_block = ""
    if sig_html:
        sig_block = (
            '<tr><td style="padding:0 36px 24px 36px">'
            '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            '<tr><td style="border-top:2px solid ' + primary + ';padding-top:16px;'
            'font-size:13px;color:#666;font-family:Arial,sans-serif;line-height:1.5">'
            + sig_html +
            "</td>"
            "</tr>"
            "</table>"
            "</td>"
        )

    footer_name = sname or "AETHERYON"

    return (
        "<!DOCTYPE html>"
        '<html lang="es" xmlns="http://www.w3.org/1999/xhtml">'
        "<head>"
        '<meta charset="UTF-8"/>'
        '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
        '<meta http-equiv="X-UA-Compatible" content="IE=edge"/>'
        "<title>Email</title>"
        "</head>"
        '<body style="margin:0;padding:0;background-color:#f0f0f0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"'
        ' style="background-color:#f0f0f0;padding:32px 16px">'
        '<tr><td align="center">'
        '<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"'
        ' style="max-width:600px;width:100%">'
        "<tr>"
        '<td style="background-color:' + header_bg + ';border-radius:10px 10px 0 0;padding:24px 36px">'
        '<span style="font-family:Georgia,serif;font-size:21px;font-weight:700;'
        "color:" + header_fc + ';letter-spacing:-0.3px;line-height:1">'
        + (sname or "") +
        "</span>"
        "</td>"
        "</tr>"
        "<tr>"
        '<td style="background-color:' + bg + ';padding:36px 36px 28px 36px;'
        "font-family:" + font + ";font-size:" + fsize + ";color:" + text_col + ';line-height:1.75">'
        + body_html +
        "</td>"
        "</tr>"
        + sig_block +
        "<tr>"
        '<td style="background-color:#f8f8f8;border-radius:0 0 10px 10px;'
        'border-top:1px solid #e4e4e4;padding:16px 36px">'
        '<p style="margin:0;font-size:12px;color:#aaa;font-family:Arial,sans-serif">'
        'Enviado desde <strong style="color:#888">' + footer_name + "</strong>."
        "</p>"
        "</td>"
        "</tr>"
        "</table>"
        "</td>"
        "</tr>"
        "</table>"
        "</body></html>"
    )


def html_to_text(html: str) -> str:
    """Fallback de texto plano a partir de HTML pegado (modo HTML de
    Redacción). No pretende ser perfecto — solo dar una alternativa legible
    para clientes de correo que no rendericen HTML."""
    if not html:
        return ""
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", "", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|tr|li|h[1-6])>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&#39;", "'")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


_STRONG_HTML_RE = re.compile(r"<!DOCTYPE\s+html|<html[\s>]|<body[\s>]|<style[\s>]", re.IGNORECASE)


def looks_like_full_html_document(text: str) -> bool:
    """Señal fuerte de que `text` ya es un documento HTML completo (no
    Markdown): DOCTYPE, <html>, <body> o un bloque <style>. Cualquier
    template HTML real exportado de un editor de emails trae al menos una
    de estas — un texto en Markdown normal, casi nunca."""
    if not text:
        return False
    return bool(_STRONG_HTML_RE.search(text.strip()))


def is_html_mode(body: str, content_type: str) -> bool:
    """Decisión única y compartida de si `body` debe tratarse como HTML
    completo (sin envolver, sin pasar por Markdown) — ya sea porque el
    modo lo pide explícitamente, o porque el contenido es inequívocamente
    un documento HTML completo aunque haya llegado marcado como 'markdown'.
    Todo lo que decide 'HTML o no' (armado del email Y su texto plano de
    respaldo) usa esta misma función, para que nunca queden desincronizados."""
    return (content_type or "markdown") == "html" or looks_like_full_html_document(body)


def assemble(body: str, content_type: str, style_cfg: dict = None) -> str:
    """Arma el HTML final del email según el modo de Redacción:
    - 'markdown' (por defecto): el cuerpo se interpreta como Markdown liviano
      y se envuelve en la plantilla visual de AETHERYON (header/firma/footer).
    - 'html': el cuerpo YA es un template diseñado y completo (pegado por el
      usuario) — se envía tal cual, sin reprocesar ni envolver, para respetar
      el diseño exactamente como fue pegado.

    Red de seguridad: si igual llega marcado como 'markdown' pero el
    contenido es inequívocamente un documento HTML completo (DOCTYPE/html/
    body/style), se lo respeta como HTML de todos modos — así una falla de
    estado en el frontend (o un cliente de API externo) nunca termina
    mandando código HTML escapado y sin renderizar dentro de la plantilla.
    """
    if is_html_mode(body, content_type):
        return body or ""
    return build_html_email(body, style_cfg)

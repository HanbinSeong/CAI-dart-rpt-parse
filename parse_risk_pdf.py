# parse_risk_pdf.py
import pdfplumber
import re
import json

# ---- 시작 앵커(맨 앞) 패턴들 ----
CHAP_RE = re.compile(r'^(\d+)\s+(.+)$')                 # "7 기타 투자위험요소 예시"
SEC_RE  = re.compile(r'^(\d+)-(\d+)\s*\(([^)]*)\)\s*(.*)$')   # "7-1 (주가 희석화) 나머지"
ART_RE  = re.compile(r'^(\d+)-(\d+)-(\d+)\.\s*(.*)$')         # "7-1-1. 본문..."

HEADER_RE     = re.compile(r'^\s*투자위험요소\s*기재요령\s*안내서')  # 반복 헤더
PAGE_LINE_RE  = re.compile(r'^\s*-\s*\d+\s*-\s*$')              # "- 5 -" 같은 줄만 제거(라인 전체 앵커)

def parse_risk_pdf(pdf_path: str):
    docs = []

    # 현재 컨텍스트(상태)
    current_chap_id   = None
    current_chap_name = None
    current_sec_id    = None
    current_sec_name  = None

    # 진행 중 문서(절/조)
    cur = None  # dict: chap_id/chap_name/sec_id/sec_name/art_id/content

    def flush():
        nonlocal cur, docs
        if cur and cur.get("chap_id") and cur.get("sec_id") and cur.get("art_id") is not None:
            # content가 비어도 구조는 유지
            cur["content"] = (cur.get("content") or "").strip()
            docs.append(cur)
        cur = None

    # 1) PDF → 텍스트
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join((page.extract_text() or "") for page in pdf.pages)

    # 2) 라인 단위 파싱 (맨 앞에서만 매칭)
    for raw in full_text.split('\n'):
        line = (raw or '').strip()
        if not line:
            continue
        if PAGE_LINE_RE.fullmatch(line):  # 페이지번호 줄만 제거 (내부 "7-1-1." 훼손 방지)
            continue
        if HEADER_RE.match(line):
            continue

        # 우선순위: ART > SEC > CHAP (모두 "맨 앞" 매칭)
        m_art = ART_RE.match(line)
        if m_art:
            flush()  # 이전 문서 종료
            chap_id, sec_id, art_id, content = m_art.groups()

            # 컨텍스트 갱신
            current_chap_id = chap_id
            # current_chap_name은 직전 장 라인에서 이미 세팅되어 있을 수 있음
            current_sec_id = sec_id
            # current_sec_name은 직전 절 라인에서 이미 세팅되어 있을 수 있음

            # 새 문서 시작
            cur = {
                "chap_id":   current_chap_id,
                "chap_name": current_chap_name,
                "sec_id":    current_sec_id,
                "sec_name":  current_sec_name,
                "art_id":    art_id,
                "content":   (content or "").strip(),
            }
            continue

        m_sec = SEC_RE.match(line)
        if m_sec:
            flush()
            chap_id, sec_id, sec_name, rest = m_sec.groups()

            # 컨텍스트 갱신
            current_chap_id   = chap_id
            current_sec_id    = sec_id
            current_sec_name  = (sec_name or "").strip()

            cur = {
                "chap_id":   current_chap_id,
                "chap_name": current_chap_name,  # 마지막으로 본 장 제목 자동 전파
                "sec_id":    current_sec_id,
                "sec_name":  current_sec_name,
                "art_id":    "0",                # 절 본문은 art_id=0
                "content":   (rest or "").strip(),
            }
            continue

        m_ch = CHAP_RE.match(line)
        if m_ch:
            # 장은 문서가 아니므로, 진행 중 문서가 있으면 종료 후 장 컨텍스트만 갱신
            flush()
            chap_id, chap_name = m_ch.groups()
            current_chap_id   = chap_id
            current_chap_name = (chap_name or "").strip()
            # 장이 바뀌면 절 컨텍스트는 초기화
            current_sec_id    = None
            current_sec_name  = None
            continue

        # 일반 라인: 현재 문서 본문에 이어붙임
        if cur:
            cur["content"] += (" " if cur["content"] else "") + line

    # 끝나면 마지막 문서 flush
    flush()
    return docs


if __name__ == "__main__":
    pdf_file_path = "./standard/투자위험요소 기재요령 안내서(202401).pdf"
    out_path = "risk_standard_bulk.json"

    parsed = parse_risk_pdf(pdf_file_path)
    print(f"총 {len(parsed)}개 항목")
    print(json.dumps(parsed[:5], ensure_ascii=False, indent=2))

    with open(out_path, "w", encoding="utf-8") as f:
        for d in parsed:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
    print(f"saved -> {out_path}")

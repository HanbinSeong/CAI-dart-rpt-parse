# parse_risk_pdf.py
import pdfplumber
import re
import json
import unicodedata

# 유니코드 대시(하이픈) 문자 클래스: ASCII - 및 Pd 주요 문자
DASH_CHARS = r"[\u002D\u2010-\u2015\u2212\uFE63\uFF0D]"
SEP = rf"(?:\s*{DASH_CHARS}\s*|\s+)"  # 하이픈 또는 공백을 구분자로 모두 허용

# ❶ 시작 고정(match) 대신 search 사용 + 분리자 관대화
chap_pattern = re.compile(rf"\b(\d+)\s+(.+)")
sec_pattern  = re.compile(rf"\b(\d+){SEP}(\d+)\s*\((.*?)\)\s*(.*)")
art_pattern  = re.compile(rf"\b(\d+){SEP}(\d+){SEP}(\d+)\.\s*(.*)")

def normalize_text(s: str) -> str:
    # ❷ 유니코드 정규화(NFKC): 숫자/기호 통일 (전각, 합성문자 등)
    #    + 비표준 대시들을 ASCII '-'로 통일
    s = unicodedata.normalize("NFKC", s)  # NFC도 가능하나 기호 통합엔 NFKC가 유리
    s = re.sub(DASH_CHARS, "-", s)
    # 흔한 제어 문자 정리
    s = s.replace("\u00A0", " ")  # NBSP
    return s

def parse_risk_guide_pdf(pdf_path: str, y_tol: float = 2.0, x_tol: float = 1.0):
    """
    '투자위험요소 기재요령 안내서' PDF를 파싱하여 구조화된 JSON 리스트로 반환
    """
    docs = []
    cur = None
    chap_id = chap_name = None
    sec_id = sec_name = None

    # ❸ 줄 병합 완화: y_tolerance를 낮춰 줄 경계 보존 유도
    with pdfplumber.open(pdf_path) as pdf:
        full_text = []
        for page in pdf.pages:
            txt = page.extract_text(y_tolerance=y_tol, x_tolerance=x_tol) or ""
            # 푸터의 "- 숫자 -" 패턴 제거
            txt = re.sub(r"-\s*\d+\s*-", "", txt)
            full_text.append(txt)
        full_text = "\n".join(full_text)

    # ❹ 유니코드/대시 정규화
    full_text = normalize_text(full_text)

    # 줄 단위 스캔하면서, 한 줄에 "섹션/조"가 여러 번 나타나도 모두 쪼갬
    for raw_line in full_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # PDF 헤더/잡음 건너뛰기
        if line.isdigit() or line.startswith("■투자위험요소 기재요령 안내서"):
            continue

        pos = 0
        while pos < len(line):
            # 현재 위치부터 가장 먼저 나오는 번호 패턴(조 > 섹션 > 챕터) 탐색
            m_art = art_pattern.search(line, pos)
            m_sec = sec_pattern.search(line, pos)
            m_ch  = chap_pattern.search(line, pos)

            # 후보 중 가장 앞에 나타난 매치 선택 (동일 시작이면 우선순위: 조 > 섹션 > 챕터)
            candidates = [(m_art, "art"), (m_sec, "sec"), (m_ch, "chap")]
            candidates = [(m, kind) for (m, kind) in candidates if m]
            if not candidates:
                # 남은 꼬리 텍스트는 현재 문서에 이어 붙임
                if cur:
                    tail = line[pos:].strip()
                    if tail:
                        cur["content"] += (" " if cur["content"] else "") + tail
                break

            m, kind = min(candidates, key=lambda t: (t[0].start(), {"art":0,"sec":1,"chap":2}[t[1]]))

            # 매치 이전의 프리픽스 텍스트는 현재 문서에 이어 붙임
            prefix = line[pos:m.start()].strip()
            if prefix and cur:
                cur["content"] += (" " if cur["content"] else "") + prefix

            if kind == "art":
                # 이전 문서 flush
                if cur: docs.append(cur)
                g1, g2, g3, content = m.groups()
                cur = {
                    "chap_id": chap_id,
                    "chap_name": chap_name,
                    "sec_id": g2,
                    "sec_name": sec_name,
                    "art_id": g3,
                    "content": content.strip()
                }

            elif kind == "sec":
                if cur: docs.append(cur)
                g1, g2, sec_nm, content = m.groups()
                chap_id = g1  # 섹션 라인에도 챕터 번호가 앞에 나타남
                sec_id = g2
                sec_name = sec_nm.strip()
                cur = {
                    "chap_id": chap_id,
                    "chap_name": chap_name,
                    "sec_id": sec_id,
                    "sec_name": sec_name,
                    "art_id": "0",
                    "content": (content or "").strip()
                }

            else:  # "chap"
                # 챕터는 상태만 갱신
                if cur: docs.append(cur)
                chap_id, chap_name = m.groups()
                chap_name = chap_name.strip()
                cur = None

            pos = m.end()

    if cur:
        docs.append(cur)

    return docs

if __name__ == "__main__":
    pdf_file_path = "./standard/투자위험요소 기재요령 안내서(202401).pdf"
    parsed = parse_risk_guide_pdf(pdf_file_path)

    print(f"총 {len(parsed)}개의 문서가 성공적으로 파싱되었습니다.")
    print(json.dumps(parsed[:5], ensure_ascii=False, indent=2))

    with open("risk_standard_bulk.json", "w", encoding="utf-8") as f:
        for doc in parsed:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print("\n'risk_standard_bulk.json' 파일이 생성되었습니다.")

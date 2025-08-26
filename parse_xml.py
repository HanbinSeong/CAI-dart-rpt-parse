# parse_xml.py
import xml.etree.ElementTree as ET
import re
import os
import json
import codecs
from bs4 import BeautifulSoup
import pandas as pd

df_codes = pd.read_csv(
    "./company_overview_codes.csv",
    dtype={"corp_code": str, "induty_code": str}
)


def preprocess_xml_content(xml_string):
    # 허용되는 태그 이름 리스트
    WHITELIST = {
        "DOCUMENT", "DOCUMENT-NAME", "FORMULA-VERSION", "COMPANY-NAME", "SUMMARY",
        "LIBRARY", "BODY", "EXTRACTION", "COVER", "COVER-TITLE",
        "IMAGE", "IMG", "IMG-CAPTION",  "P", "A", "SPAN",
        "TR", "TD", "TH", "TE", "TU", "SECTION-1", "SECTION-2", "SECTION-3",
        "TITLE", "TABLE", "TABLE-GROUP", "COLGROUP", "COL", "THEAD",
        "TBODY", "PGBRK", "PART", "CORRECTION"
    }
    # TAG_RE = re.compile(
    #     r"<(/?)([A-Za-z0-9\-]+)"               # 그룹 1: 슬래시?, 그룹 2: 태그명
    #     r"([^>]*)"                             # 그룹 3: 속성 등
    #     r">"
    # )
    # 태그 이름만 추출하는 정규식: &lt;/TD&gt; -> /TD, &lt;TD ...&gt; -> TD ...
    TAG_RE = re.compile(r"&lt;(/?)(\w+(?:-\w+)*)([^&]*)&gt;")
    def restore_whitelisted_tags(match):
        slash, tag, attrs = match.groups()
        if tag.upper() in WHITELIST:
            return f"<{slash}{tag}{attrs}>"
        return match.group(0) # 화이트리스트에 없으면 그대로 둠
    
    def repl(m):
        slash, tag, attrs = m.group(1), m.group(2), m.group(3)
        if tag not in WHITELIST:
            # 전체 태그 엔티티 처리
            inner = m.group(0)[1:-1]  # "<...>" 사이 문자열
            return "&lt;" + inner + "&gt;"
        else:
            # 허용된 태그: 속성은 그대로 유지
            return f"<{slash}{tag}{attrs}>"

    # 1. XML 선언 (Processing Instruction) 추출 및 보호
    xml_declaration_match = re.match(r"<\?xml[^>]*\?>\s*", xml_string)
    xml_declaration = ""
    remaining_xml_string = xml_string

    if xml_declaration_match:
        xml_declaration = xml_declaration_match.group(
            0
        )  # 매치된 선언과 뒤따르는 공백 포함
        remaining_xml_string = xml_string[
            xml_declaration_match.end() :
        ]  # 선언 부분 제거

    encoded_xml = remaining_xml_string.replace("<", "&lt;").replace(">", "&gt;")
    cleaned_xml = TAG_RE.sub(restore_whitelisted_tags, encoded_xml)

    # SPAN(스타일), A(링크) 태그 제거
    span_pattern = re.compile(r'</?SPAN\b[^>]*?>', re.IGNORECASE)
    cleaned_xml_string = span_pattern.sub('', cleaned_xml)
    a_pattern = re.compile(r'</?A\b[^>]*?>', re.IGNORECASE)
    cleaned_xml_string = a_pattern.sub('', cleaned_xml_string)

    # 2. 전체 문자열 앞뒤의 공백 및 개행 문자 제거 (XML 선언 제외한 나머지 부분에 적용)
    cleaned_xml_string = cleaned_xml_string.strip()

    # 3. XML 1.0 사양에서 허용되지 않는 제어 문자 제거 (ParseError 방지)
    cleaned_xml_string = re.sub(
        r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", cleaned_xml_string
    )

    # 4. 이스케이프되지 않은 '&' 문자를 '&amp;'로 변환
    cleaned_xml_string = re.sub(r'&(?!#|amp;)', r'&amp;', cleaned_xml_string)

    if xml_declaration:
        # 원래 선언 뒤에 공백이나 개행이 있었다면 그것을 유지
        # 새로운 시작 문자열 앞에도 개행을 넣어 줄 맞춤을 시도합니다.
        cleaned_xml_string = xml_declaration + "\n" + cleaned_xml_string.lstrip()
    # print(cleaned_xml_string)

    return cleaned_xml_string


def clean_table_html_for_llm(html_string):
    """
    LLM에 전달하기 위한 테이블 HTML에서 불필요한 레이아웃/스타일 속성을 제거하고
    비표준 태그 (TE, TU)를 표준 TD로 변환합니다.
    ROWSPAN, COLSPAN과 같은 구조 관련 속성은 유지합니다.
    또한, HTML 문자열 내의 불필요한 줄바꿈과 역슬래시를 제거합니다.
    """
    soup = BeautifulSoup(html_string, "html.parser")

    # 불필요한 속성 제거 목록 (소문자로 비교)
    # LLM에게는 불필요하거나 시각적 정보인 속성들
    attrs_to_remove = [
        "width",
        "height",
        "align",
        "valign",
        "aclass",
        "afixtable",
        "acopy",
        "adelete",
        "aupdatecont",
        "acopycol",
        "amovecol",
        "adeletecol",
        "usermark",
        "acode",
        "aunit",
        "aunitvalue",
        "refno",
        "aassocnote",
        "atoc",
        "atocid",
        "adelim",  # DART 특유의 메타데이터 속성들
        "border",
        "frame",
        "rules",  # 요청에 따라 추가
        "style",
        "class",
        "id",  # 일반적으로 HTML에서 불필요한 속성 추가
    ]

    for tag in soup.find_all(True):  # 모든 태그 순회 (자신 포함)
        # 비표준 태그 (TE, TU)를 표준 TD로 변환
        if tag.name == "te" or tag.name == "tu":
            tag.name = "td"

        # 불필요한 속성 제거
        for attr_name in list(tag.attrs.keys()):
            if attr_name.lower() in attrs_to_remove:
                del tag.attrs[attr_name]

    cleaned_html = str(soup)
    # 고객님의 제안대로 순서대로 처리: 줄바꿈 제거 -> 역슬래시 제거 -> 연속 공백 압축
    cleaned_html = cleaned_html.replace("\n", "")  # 모든 줄바꿈 제거
    cleaned_html = cleaned_html.replace('\\', '')  # 모든 역슬래시 제거

    return cleaned_html.strip()  # 최종적으로 앞뒤 공백 제거


def extract_content_recursive(element, collected_items):
    """
    주어진 Element와 그 하위 Element들을 재귀적으로 탐색하여
    BORDER="1"인 TABLE은 HTML로 (정제 후), 그 외의 텍스트는 일반 텍스트로 collected_items에 추가합니다.
    """
    # BORDER="1"인 TABLE을 발견하면 HTML로 추출하고 이 가지의 탐색은 중단
    if element.tag == "TABLE" and element.get("BORDER") == "1":
        table_html = ET.tostring(element, encoding="utf-8").decode("utf-8").strip()
        cleaned_table_html = clean_table_html_for_llm(
            table_html
        )  # LLM을 위해 HTML 정제
        collected_items.append(
            {"type": "table", "content": cleaned_table_html}
        )  # 타입 추가
        return

    # 현재 엘리먼트의 직접적인 텍스트 노드 처리
    if element.text and element.text.strip():
        # 텍스트 내부의 여러 공백(개행 포함)을 단일 공백으로 정규화
        normalized_text = re.sub(r"\s+", " ", element.text.strip())
        if normalized_text:  # 비어있지 않은 경우에만 추가
            collected_items.append({"type": "text", "content": normalized_text})

    # 자식 엘리먼트들을 재귀적으로 탐색
    for child in element:
        extract_content_recursive(child, collected_items)

        # 자식 엘리먼트의 tail 텍스트 노드 처리
        if child.tail and child.tail.strip():
            # tail 텍스트 내부의 여러 공백(개행 포함)을 단일 공백으로 정규화
            normalized_tail_text = re.sub(r"\s+", " ", child.tail.strip())
            if normalized_tail_text:  # 비어있지 않은 경우에만 추가
                collected_items.append(
                    {"type": "text", "content": normalized_tail_text}
                )

def parse_darter_xml(xml_content, file_name):
    """
    DART 공시보고서 XML 내용을 파싱하고 주요 정보를 추출합니다.
    SECTION-1과 SECTION-2를 중첩 반복문으로 처리합니다.
    """
    processed_xml_content = preprocess_xml_content(xml_content)

    try:
        root = ET.fromstring(processed_xml_content)
    except ET.ParseError as e:
        # 오류 처리 로직은 기존과 동일
        print(f"XML 파싱 오류 발생: {e}")
        return None
    

    # 스킵 조건: <TITLE> 중 텍스트가 "증권발행조건확정"인 경우
    skip_title = root.find(".//TITLE")
    if skip_title is not None and skip_title.text and skip_title.text.strip() == "증권발행조건확정":
        return None   # 🚨 이 문서는 처리하지 않음


    # 최상위 레벨 데이터 추출 (기존 코드와 동일)
    doc_id = file_name.split(".")[0]
    pub_date = file_name[:8]

    # ... (doc_name, doc_code, corp_code, corp_name 추출 로직) ...
    doc_name_element = root.find("DOCUMENT-NAME")
    doc_name = doc_name_element.text.strip() if doc_name_element is not None else ""
    doc_code = doc_name_element.get("ACODE") if doc_name_element is not None else ""
    company_name_element = root.find("COMPANY-NAME")
    corp_code = (
        company_name_element.get("AREGCIK") if company_name_element is not None else ""
    )
    corp_name = (
        company_name_element.text.strip() if company_name_element is not None else ""
    )
    induty_code = get_induty_code(corp_code)
    # ...

    report_data = {
        "doc_id": doc_id,
        "doc_name": doc_name,
        "doc_code": doc_code,
        "pub_date": pub_date,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "induty_code": induty_code,
        "sections": [],
    }

    sec_id_counter = 0

    # 1. SECTION-1 엘리먼트를 찾아서 순회
    section1_elements = root.findall(".//SECTION-1")

    for section1_element in section1_elements:
        # SECTION-1의 제목을 추출
        title1_element = section1_element.find("TITLE")

        if title1_element is not None and title1_element.text and title1_element.text.strip():
            sec_id_counter += 1
            section1_data = {
                "sec_id": f"{sec_id_counter}",
                "sec_title": title1_element.text.strip(),
                "sec_content": "",
            }

            collected_items_for_section1 = []
            
            # SECTION-1의 자식들을 순회 (SECTION-2 제외)
            for child_of_section1 in section1_element:
                if child_of_section1.tag == "SECTION-2":
                    # SECTION-2는 별도로 처리하므로 건너뛰기
                    continue
                if child_of_section1.tag == "TITLE":
                    continue
                extract_content_recursive(child_of_section1, collected_items_for_section1)
            
            # SECTION-1의 콘텐츠를 합치고 저장
            section1_data["sec_content"] = _combine_contents(collected_items_for_section1)
            report_data["sections"].append(section1_data)

        # 2. SECTION-1 아래에 있는 SECTION-2 엘리먼트를 찾아서 순회
        section2_elements = section1_element.findall("./SECTION-2")

        for section2_element in section2_elements:
            # SECTION-2의 제목을 추출
            title2_element = section2_element.find("TITLE")

            if title2_element is not None and title2_element.text and title2_element.text.strip():
                sec_id_counter += 1
                section2_data = {
                    "sec_id": f"{sec_id_counter}",
                    "sec_title": title2_element.text.strip(),
                    "sec_content": "",
                }

                collected_items_for_section2 = []
                
                # SECTION-2의 자식들을 순회 (SECTION-3부터는 모두 콘텐츠)
                for child_of_section2 in section2_element:
                    if child_of_section2.tag == "TITLE":
                        continue
                    extract_content_recursive(child_of_section2, collected_items_for_section2)
                
                # SECTION-2의 콘텐츠를 합치고 저장
                section2_data["sec_content"] = _combine_contents(collected_items_for_section2)
                report_data["sections"].append(section2_data)

    return report_data

def _combine_contents(items):
    """
    콘텐츠 아이템 리스트를 받아서 하나의 문자열로 합치는 헬퍼 함수
    (기존 코드의 콘텐츠 합치는 로직을 별도 함수로 분리)
    """
    final_sec_content_parts_builder = []
    prev_type = None

    for item in items:
        current_content = item["content"]
        current_type = item["type"]
        if final_sec_content_parts_builder:
            if prev_type != current_type or (
                current_type == "text" and not current_content.strip()
            ):
                if not final_sec_content_parts_builder[-1].endswith("\n"):
                    final_sec_content_parts_builder.append("\n")
        
        final_sec_content_parts_builder.append(current_content)
        prev_type = current_type

    raw_final_content = "".join(final_sec_content_parts_builder).strip()
    final_sec_content = re.sub(r"\s+", " ", raw_final_content)
    final_sec_content = re.sub(r"\n+", "\n", final_sec_content)
    final_sec_content = final_sec_content.strip()
    return final_sec_content

# if __name__ == "__main__":
#     # 파싱오류 발생 XML 파일들
#     # XML_FILE_PATH = "20240430000817.xml"
#     # XML_FILE_PATH = "20240516000056.xml"
#     # XML_FILE_PATH = "20240514001094.xml"
#     # XML_FILE_PATH = "20240514001108.xml"
#     # XML_FILE_PATH = "20240524000535.xml"
#     XML_FILE_PATH = "20240110000519.xml"
#     with codecs.open(XML_FILE_PATH, "r", encoding="utf-8") as f:
#         xml_content = f.read()
#     parsed_data = parse_darter_xml(xml_content, os.path.basename(XML_FILE_PATH))
#     if parsed_data:
#         print(json.dumps(parsed_data, ensure_ascii=False, indent=2))
#     else:
#         print("Parsing failed.")




def get_induty_code(corp_code: str) -> str:
    """corp_code를 입력받아 induty_code 반환"""
    row = df_codes[df_codes['corp_code'] == corp_code]
    if not row.empty:
        return row.iloc[0]['induty_code']
    return None  # 없으면 None 반환
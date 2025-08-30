# parse_pdf.py
import pdfplumber
import re
import json


def parse_pdf(pdf_path):
    """
    기업공시작성기준 PDF 파일을 '장', '절', '조' 단위로 파싱하는 함수.

    Args:
        pdf_path (str): 파싱할 PDF 파일의 경로.

    Returns:
        list: 파싱된 데이터가 담긴 딕셔너리 리스트.
    """

    # 파싱된 데이터를 저장할 리스트
    parsed_data = []

    # 현재 상태를 추적할 변수 초기화
    current_chap_id = None
    current_chap_name = None
    current_sec_id = None
    current_sec_name = None
    current_art_id = None
    current_art_name = None
    current_content = ""

    # 정규표현식 패턴 정의
    CHAP_PATTERN = re.compile(r"^제\s*(\d+)\s*장\s*(.*)$")
    SEC_PATTERN = re.compile(r"^제\s*(\d+)\s*절\s*(.*)$")
    # 'art_name'을 괄호 안의 텍스트만 추출하도록 수정
    ART_PATTERN = re.compile(r"^제\s*(\d+)-(\d+)-(\d+)\s*조\s*\((.*?)\).*$")

    # 별지, 부칙 시작을 알리는 패턴 (파싱 제외)
    # '◈ 별지'와 '부 칙'을 모두 정확하게 매칭하도록 수정
    EXCLUDE_PATTERN = re.compile(r"^(◈\s*별지\s*:\s*공시서식|부\s*칙)", re.MULTILINE)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다. 경로를 확인해주세요: {pdf_path}")
        return []

    # 전체 텍스트를 줄 단위로 분리하여 순회
    lines = full_text.split("\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 목차가 아닌 본문에서 별지, 부칙 패턴을 만나면 파싱 중단
        if EXCLUDE_PATTERN.match(line) and len(parsed_data) > 0:
            print("본문에서 별지 또는 부칙 섹션이 시작되어 파싱을 중단합니다.")
            break

        # '조' 패턴을 가장 먼저 확인
        art_match = ART_PATTERN.match(line)
        if art_match:
            if current_art_id is not None:
                parsed_data.append(
                    {
                        "chap_id": current_chap_id,
                        "chap_name": current_chap_name,
                        "sec_id": current_sec_id,
                        "sec_name": current_sec_name,
                        "art_id": current_art_id,
                        "art_name": current_art_name,
                        "content": current_content.strip(),
                    }
                )

            # 새로운 '조' 정보 업데이트
            # current_art_id = f"{art_match.group(1)}-{art_match.group(2)}-{art_match.group(3)}"
            current_art_id = art_match.group(3)
            current_art_name = art_match.group(4)
            current_content = ""
            continue

        # '절' 패턴 확인
        sec_match = SEC_PATTERN.match(line)
        if sec_match:
            if current_art_id is not None:
                parsed_data.append(
                    {
                        "chap_id": current_chap_id,
                        "chap_name": current_chap_name,
                        "sec_id": current_sec_id,
                        "sec_name": current_sec_name,
                        "art_id": current_art_id,
                        "art_name": current_art_name,
                        "content": current_content.strip(),
                    }
                )

            current_sec_id = sec_match.group(1)
            current_sec_name = sec_match.group(2)
            current_art_id = None
            current_art_name = None
            current_content = ""
            continue

        # '장' 패턴 확인
        chap_match = CHAP_PATTERN.match(line)
        if chap_match:
            if current_art_id is not None:
                parsed_data.append(
                    {
                        "chap_id": current_chap_id,
                        "chap_name": current_chap_name,
                        "sec_id": current_sec_id,
                        "sec_name": current_sec_name,
                        "art_id": current_art_id,
                        "art_name": current_art_name,
                        "content": current_content.strip(),
                    }
                )

            current_chap_id = chap_match.group(1)
            current_chap_name = chap_match.group(2)

            current_sec_id = "1"
            current_sec_name = current_chap_name
            current_art_id = None
            current_art_name = None
            current_content = ""
            continue

        # 패턴에 해당하지 않는 경우, 현재 '조'의 내용에 추가
        current_content += "\n" + line

    if current_art_id is not None:
        parsed_data.append(
            {
                "chap_id": current_chap_id,
                "chap_name": current_chap_name,
                "sec_id": current_sec_id,
                "sec_name": current_sec_name,
                "art_id": current_art_id,
                "art_name": current_art_name,
                "content": current_content.replace("\n", " ").strip(),
            }
        )

    return parsed_data


# # 사용 예시
# pdf_path = "./standard/(붙임4) 기업공시서식 작성기준(2025.6.30. 시행).pdf"
# parsed_result = parse_pdf(pdf_path)

# # 결과 출력 (보기 좋게 JSON 형식으로)
# print(json.dumps(parsed_result, indent=2, ensure_ascii=False))

import pdfplumber

pdf_path = "./standard/투자위험요소 기재요령 안내서(202401).pdf"
# pdf_path = "./standard/투자위험요소 기재요령 안내서('24.1.).pdf"

try:
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
except FileNotFoundError:
    print(f"오류: 파일을 찾을 수 없습니다. 경로를 확인해주세요: {pdf_path}")

# print(full_text)
# 전체 텍스트를 줄 단위로 분리하여 순회
lines = full_text.split('\n')
# print(lines)

for line in lines:
    line = line.strip()
    if not line:
        continue
    # - 40 - 같은 페이지 번호 행 제거
    elif line.startswith("-") and line.endswith("-") and len(line) <= 7:
        continue
    print(line)
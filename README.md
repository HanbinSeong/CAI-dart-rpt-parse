# 디렉토리 구조dd
```
├── report/                       # 공시 원본(XML) 루트
│   ├── 1분기/                    # 1분기 보고서 XML (e.g. 1분기/20140101000000/20140101000000.xml)
│   ├── 3분기/                    # 3분기 보고서 XML
│   ├── 반기/                     # 반기 보고서 XML
│   ├── 사업/                     # 사업보고서 XML
│   └── 증권/                     # 증권신고서 XML
├── standard/                     # 기업공시작성기준 PDF
│   └── *.pdf
├── ingest_to_os_from_pdf.py      # 표준(PDF) → OpenSearch 인제스트
├── ingest_to_os_from_xml.py      # 공시 XML → OpenSearch 인제스트
├── parse_pdf.py                  # PDF 파서(장/절/조 단위 분해)
├── parse_xml.py                  # XML 파서(DART 문서 전처리·파싱)
├── requirements.txt              # 필요 라이브러리
```
---

# 환경구성
```
# 가상환경 생성
python -m venv {가상환경명}

# 활성화
{가상환경명}/Scripts/activate

# 라이브러리 설치
pip install -r requirements.txt
```
---

# 스크립트 실행
```
# PDF -> OpenSearch DB 주입
python ingest_to_os_from_pdf.py

# XML -> OpenSeaerch DB 주입
python ingest_to_os_from_xml.py
```
---

# 데이터 확인
`localhost:5601` 접속 -> 좌측 메뉴탭 -> 맨 아래 `Dev Tools`
아래 스크립트 복붙해서 원하는 부분에 `Ctrl + Enter`

```
GET /rpt_other/_search
{
  "query": {
    "match_all": {}
  }
}

GET /rpt_sec_eq/_search
{
  "query": {
    "match": {
      "doc_id": 20240110000519
    }
  }
}

GET /rpt_qt/_search
{
  "query": {
    "nested": {
      "path": "sections",
      "query": {
        "match": {
          "sections.sec_title": "개요"
        }
      },
      "inner_hits": {}
    }
  }
}

GET /rpt_sec_eq/_search
{
  "from": 0,
  "size": 1,
  "query": {
    "nested": {
      "path": "sections",
      "query": {
        "match": {
          "sections.sec_content": "삼성 반도체 노트북"
        }
      },
      "inner_hits": {}
    }
  }
}

GET /rpt_qt/_search
{
  "_source": false, 
  "from": 0,
  "size": 5, 
  "query": {
    "nested": {
      "path": "sections",
      "query": {
        "bool": {
        "should": [
          { "match": { "sections.sec_title": {"query": "위험 요소", "boost": 1.0} } },
          { "match": { "sections.sec_content": {"query": "트럼프 관세 전쟁", "boost": 1.0} } }
        ]
      }
      },
      "inner_hits": {
        "_source": ["sections.sec_id", "sections.sec_title", "sections.sec_content"]
      }
    }
  }
}


GET /standard/_search
{
  "query": {
    "match": {
      "content": "증권신고서"
    }
  }
}





GET /rpt_qt/_count
GET /rpt_half/_count
GET /rpt_biz/_count
GET /rpt_sec_eq/_count
GET /rpt_other/_count
GET /standard/_count

GET /rpt_qt
GET /rpt_half
GET /rpt_biz
GET /rpt_sec_eq
GET /rpt_other
GET /standard

DELETE /rpt_qt
DELETE /rpt_half
DELETE /rpt_biz
DELETE /rpt_sec_eq
DELETE /rpt_other

```
# -*- coding: utf-8 -*-
# ingest_to_os_from_risk_pdf.py (투자위험요소 기재요령 안내서)
import os
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from parse_risk_pdf import parse_risk_pdf  # ← 파서 연동

# from dotenv import load_dotenv
# load_dotenv()

# ─────────────────────────────────────────────
# OpenSearch 접속 정보
# ─────────────────────────────────────────────
OS_HOSTS = os.getenv("OS_HOSTS", "http://localhost:9200").split(",")
OS_USER = os.getenv("OS_USER") or None
OS_PASS = os.getenv("OS_PASS") or None

os_client = OpenSearch(
    hosts=OS_HOSTS,
    http_compress=True,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
    http_auth=(OS_USER, OS_PASS) if OS_USER and OS_PASS else None,
)

# ─────────────────────────────────────────────
# 인덱스 매핑/설정
#  - content만 text, 나머지는 keyword
#  - shards=5, replicas=0
# ─────────────────────────────────────────────
INDEX_NAME = "risk_standard"

INDEX_BODY = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "ko_nori": {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["lowercase", "nori_part_of_speech"],
                },
                "default": {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["lowercase", "nori_part_of_speech"],
                },
                "default_search": {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["lowercase", "nori_part_of_speech"],
                },
            }
        },
    },
    "mappings": {
        "properties": {
            "chap_id": {"type": "keyword"},
            "chap_name": {
                "type": "text",
                "analyzer": "ko_nori",
                "search_analyzer": "ko_nori",
            },
            "sec_id": {"type": "keyword"},
            "sec_name": {
                "type": "text",
                "analyzer": "ko_nori",
                "search_analyzer": "ko_nori",
            },
            "art_id": {"type": "keyword"},
            "content": {
                "type": "text",
                "analyzer": "ko_nori",
                "search_analyzer": "ko_nori",
            },
        }
    },
}


def ensure_index(index_name: str):
    """인덱스가 없으면 생성."""
    if not os_client.indices.exists(index=index_name):
        print(f"[create] index '{index_name}'")
        os_client.indices.create(index=index_name, body=INDEX_BODY)
    else:
        print(f"[exists] index '{index_name}'")


def generate_actions(index_name: str, parsed):
    """bulk()용 제너레이터."""
    for doc in parsed:
        yield {
            "_index": index_name,
            "_id": f"{doc.get('chap_id','')}-{doc.get('sec_id','')}-{doc.get('art_id','')}",
            "_source": doc,
        }


def ingest_documents(index_name: str, documents: list):
    if not documents:
        print("No documents to ingest.")
        return
    print(f"Ingest {len(documents)} docs → '{index_name}'")
    success, errors = bulk(
        os_client,
        generate_actions(index_name, documents),
        refresh=False,  # 필요하면 True
        request_timeout=60,
    )
    print(f"bulk result: success={success}, errors={len(errors) if errors else 0}")
    if errors:
        # per-item 에러 첫 건 출력
        print("first error:", errors[0])


if __name__ == "__main__":
    ensure_index(INDEX_NAME)

    # 파싱할 PDF 경로
    pdf_file_path = "./standard/투자위험요소 기재요령 안내서(202401).pdf"
    if not os.path.exists(pdf_file_path):
        raise SystemExit(f"PDF not found: {pdf_file_path}")

    print(f"Parsing: {pdf_file_path}")
    parsed = parse_risk_pdf(pdf_file_path)
    ingest_documents(INDEX_NAME, parsed)

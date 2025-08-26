# ingest_to_os.py  (기존 ingest_to_es.py 대체)

import os
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from parse_xml import parse_darter_xml
import codecs

# OpenSearch 접속 정보
OS_HOSTS = ["http://localhost:9200"]  # OpenSearch 노드 URL
os_client = OpenSearch(
    hosts=OS_HOSTS,
    http_compress=True,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
)

DOC_CODE_INDEX_MAP = {
    "11013": "rpt_qt",
    "11012": "rpt_half",
    "11011": "rpt_biz",
    "10001": "rpt_sec_eq",
    "99999": "rpt_other",
}

INDEX_MAPPINGS = {
    "rpt_qt": {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_html_strip_analyzer": {
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "doc_name": {"type": "keyword"},
                "doc_code": {"type": "keyword"},
                "pub_date": {"type": "date", "format": "yyyyMMdd"},
                "corp_code": {"type": "keyword"},
                "corp_name": {"type": "keyword"},
                "induty_code": {"type": "keyword"},
                "sections": {
                    "type": "nested",
                    "properties": {
                        "sec_id": {"type": "keyword"},
                        "sec_title": {"type": "text"},
                        "sec_content": {
                            "type": "text",
                            "analyzer": "my_html_strip_analyzer",
                        },
                    },
                },
            }
        },
    },
    "rpt_half": {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_html_strip_analyzer": {
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "doc_name": {"type": "keyword"},
                "doc_code": {"type": "keyword"},
                "pub_date": {"type": "date", "format": "yyyyMMdd"},
                "corp_code": {"type": "keyword"},
                "corp_name": {"type": "keyword"},
                "induty_code": {"type": "keyword"},
                "sections": {
                    "type": "nested",
                    "properties": {
                        "sec_id": {"type": "keyword"},
                        "sec_title": {"type": "text"},
                        "sec_content": {
                            "type": "text",
                            "analyzer": "my_html_strip_analyzer",
                        },
                    },
                },
            }
        },
    },
    "rpt_biz": {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_html_strip_analyzer": {
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "doc_name": {"type": "keyword"},
                "doc_code": {"type": "keyword"},
                "pub_date": {"type": "date", "format": "yyyyMMdd"},
                "corp_code": {"type": "keyword"},
                "corp_name": {"type": "keyword"},
                "induty_code": {"type": "keyword"},
                "sections": {
                    "type": "nested",
                    "properties": {
                        "sec_id": {"type": "keyword"},
                        "sec_title": {"type": "text"},
                        "sec_content": {
                            "type": "text",
                            "analyzer": "my_html_strip_analyzer",
                        },
                    },
                },
            }
        },
    },
    "rpt_sec_eq": {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_html_strip_analyzer": {
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "doc_name": {"type": "keyword"},
                "doc_code": {"type": "keyword"},
                "pub_date": {"type": "date", "format": "yyyyMMdd"},
                "corp_code": {"type": "keyword"},
                "corp_name": {"type": "keyword"},
                "induty_code": {"type": "keyword"},
                "sections": {
                    "type": "nested",
                    "properties": {
                        "sec_id": {"type": "keyword"},
                        "sec_title": {"type": "text"},
                        "sec_content": {
                            "type": "text",
                            "analyzer": "my_html_strip_analyzer",
                        },
                    },
                },
            }
        },
    },
    "rpt_other": {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_html_strip_analyzer": {
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "doc_name": {"type": "keyword"},
                "doc_code": {"type": "keyword"},
                "pub_date": {"type": "date", "format": "yyyyMMdd"},
                "corp_code": {"type": "keyword"},
                "corp_name": {"type": "keyword"},
                "induty_code": {"type": "keyword"},
                "sections": {
                    "type": "nested",
                    "properties": {
                        "sec_id": {"type": "keyword"},
                        "sec_title": {"type": "text"},
                        "sec_content": {
                            "type": "text",
                            "analyzer": "my_html_strip_analyzer",
                        },
                    },
                },
            }
        },
    },
}

def create_indices():
    """사전 정의 인덱스 생성(있으면 skip)"""
    for index_name, mapping in INDEX_MAPPINGS.items():
        if not os_client.indices.exists(index=index_name):
            print(f"Creating index '{index_name}' with mapping...")
            body = {
                "settings": {
                    "number_of_shards": 5,
                    "number_of_replicas": 0,
                    **mapping.get("settings", {}),
                },
                "mappings": mapping.get("mappings", {}),
            }
            os_client.indices.create(index=index_name, body=body)
        else:
            print(f"Index '{index_name}' already exists.")

def generate_actions(data_dir):
    """디렉터리 하위 XML들을 파싱하고 bulk 액션 생성"""
    root_dirs = ["1분기", "3분기", "반기", "사업", "증권"]

    for folder_name in root_dirs:
        full_dir_path = os.path.join(data_dir, folder_name)
        if not os.path.isdir(full_dir_path):
            print(f"Directory not found: {full_dir_path}")
            continue

        print(f"Processing directory: {full_dir_path}")

        for root, dirs, files in os.walk(full_dir_path):
            for file_name in files:
                if file_name.endswith(".xml"):
                    file_path = os.path.join(root, file_name)

                    try:
                        with codecs.open(file_path, "r", encoding="utf-8") as f:
                            xml_content = f.read()

                        parsed_data = parse_darter_xml(xml_content, file_name)
                        if parsed_data:
                            doc_code = parsed_data.get("doc_code", "99999")
                            target_index = DOC_CODE_INDEX_MAP.get(doc_code, "rpt_other")

                            yield {
                                "_index": target_index,
                                "_id": parsed_data["doc_id"],
                                "_source": parsed_data,
                            }

                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")
                        continue
                        
    print("All folders have been processed.")

def main():
    create_indices()

    data_raw_path = "./report"
    if not os.path.isdir(data_raw_path):
        print(f"Error: The directory '{data_raw_path}' does not exist.")
        return

    print("Starting data ingestion using bulk API...")
    try:
        success, failed = bulk(
            os_client,
            generate_actions(data_raw_path),
            chunk_size=500,
            stats_only=True
        )
        print(f"Bulk ingestion completed. Succeeded: {success}, Failed: {failed}")
    except Exception as e:
        print(f"An error occurred during bulk ingestion: {e}")

if __name__ == "__main__":
    main()

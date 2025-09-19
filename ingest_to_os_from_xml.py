# ingest_to_os.py

import os
import sys
import math
from collections import defaultdict
from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
from parse_xml import parse_darter_xml
import codecs
from dotenv import load_dotenv
load_dotenv()

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

print(f"Connected to OpenSearch at {OS_HOSTS}")

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

ROOT_DIRS = ["1분기", "3분기", "반기", "사업", "증권"]

# ─────────────────────────────────────────────
# 유틸: 진행률 문자열
# ─────────────────────────────────────────────
def _pct(cur: int, total: int) -> float:
    return 0.0 if total == 0 else (cur / total * 100.0)

def _progress_line(prefix: str, cur: int, total: int) -> str:
    pct = _pct(cur, total)
    bar_len = 28
    filled = 0 if total == 0 else int(bar_len * cur / total)
    bar = "#" * filled + "-" * (bar_len - filled)
    return f"{prefix} [{bar}] {pct:5.1f}%  ({cur}/{total})"

def _print_inline(line: str):
    print("\r" + line, end="", flush=True)

# ─────────────────────────────────────────────
# 인덱스 생성
# ─────────────────────────────────────────────
def create_indices():
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

# ─────────────────────────────────────────────
# 파일 개수 카운트 (총량 계산)
# ─────────────────────────────────────────────
def count_xml_files(data_dir: str):
    folder_counts = {}
    for folder in ROOT_DIRS:
        path = os.path.join(data_dir, folder)
        cnt = 0
        if os.path.isdir(path):
            for _, _, files in os.walk(path):
                cnt += sum(1 for f in files if f.lower().endswith(".xml"))
        folder_counts[folder] = cnt
    total = sum(folder_counts.values())
    return folder_counts, total

# ─────────────────────────────────────────────
# 액션 제너레이터 (파싱 진행률 표시)
#  - id2dir_map: {doc_id -> top-level folder name} 저장 (인덱싱 진행률에 활용)
# ─────────────────────────────────────────────
def generate_actions(data_dir, folder_counts, total_files, id2dir_map):
    for folder_name in ROOT_DIRS:
        full_dir_path = os.path.join(data_dir, folder_name)
        if not os.path.isdir(full_dir_path):
            print(f"Directory not found: {full_dir_path}")
            continue

        local_total = folder_counts.get(folder_name, 0)
        if local_total == 0:
            print(f"Processing directory: {full_dir_path}")
            print("  (no .xml files)\n")
            continue

        print(f"Processing directory: {full_dir_path}")
        local_seen = 0

        for root, _, files in os.walk(full_dir_path):
            for file_name in files:
                if not file_name.lower().endswith(".xml"):
                    continue
                file_path = os.path.join(root, file_name)

                # 파싱 시도 직전에 파싱 진행률(파일 단위) 업데이트
                local_seen += 1
                global_seen = generate_actions.global_seen + 1  # 미리 +1 가정하여 표시만
                line = (
                    _progress_line("  DIR  ", local_seen, local_total)
                    + "  |  "
                    + _progress_line("TOTAL", global_seen, total_files)
                )
                _print_inline(line)

                try:
                    with codecs.open(file_path, "r", encoding="utf-8") as f:
                        xml_content = f.read()

                    parsed_data = parse_darter_xml(xml_content, file_name)
                    if parsed_data:
                        doc_code = parsed_data.get("doc_code", "99999")
                        target_index = DOC_CODE_INDEX_MAP.get(doc_code, "rpt_other")
                        doc_id = parsed_data["doc_id"]

                        # 인덱싱 단계 진행률용: doc_id -> 폴더명 매핑 보관
                        id2dir_map[doc_id] = folder_name

                        # 제너레이터에서 실제 1건 생성 완료로 카운터 증가
                        generate_actions.global_seen += 1

                        yield {
                            "_index": target_index,
                            "_id": doc_id,
                            "_source": parsed_data,
                        }

                except Exception as e:
                    # 파싱 실패도 진행은 계속
                    print(f"\nError processing file {file_path}: {e}")

        print()  # 디렉터리 종료 후 개행

# 제너레이터의 전역 진행 카운터 (속성으로 보관)
generate_actions.global_seen = 0

# ─────────────────────────────────────────────
# 메인
#  - 파싱 진행률 + 인덱싱 진행률 모두 출력
# ─────────────────────────────────────────────
def main():
    create_indices()

    data_raw_path = "./report"
    if not os.path.isdir(data_raw_path):
        print(f"Error: The directory '{data_raw_path}' does not exist.")
        return

    # 1) 총량 계산 (퍼센트 분모)
    folder_counts, total_files = count_xml_files(data_raw_path)
    print("Planned workload per folder:")
    for k, v in folder_counts.items():
        print(f"  - {k}: {v} files")
    print(f"TOTAL: {total_files} files\n")

    # 2) 액션 제너레이터 준비 (파싱 진행률은 제너레이터에서 실시간 출력)
    id2dir_map = {}  # 인덱싱 단계에서 폴더별 진행률 표시용
    actions = generate_actions(data_raw_path, folder_counts, total_files, id2dir_map)

    # 3) 인덱싱 진행률 (streaming_bulk로 실제 결과 확인)
    print("Starting data ingestion (streaming_bulk)...")
    success = 0
    failed = 0
    per_dir_done = defaultdict(int)

    for ok, item in streaming_bulk(
        os_client,
        actions,
        chunk_size=30,
        max_retries=3,
        raise_on_error=False,
        # request_timeout 등 추가 파라미터가 필요하면 여기서 지정
    ):
        meta = next(iter(item.values()))  # {'index': {...}} 형태
        doc_id = meta.get("_id")
        folder = id2dir_map.get(doc_id, "UNKNOWN")

        if ok:
            success += 1
            per_dir_done[folder] += 1
        else:
            failed += 1

        done = success + failed
        # 분모로 total_files(발견된 xml 수)를 사용합니다. (일부 파싱 실패 시 퍼센트가 100%보다 낮게 끝날 수 있음)
        line = (
            _progress_line("INDEX", done, total_files)
            + f"  |  ok={success}, fail={failed}"
        )
        _print_inline(line)

    print("\nBulk ingestion completed.")
    print(f"  - Succeeded: {success}")
    print(f"  - Failed   : {failed}")
    if per_dir_done:
        print("Per-folder indexed counts:")
        for k in ROOT_DIRS:
            if k in per_dir_done:
                print(f"  - {k}: {per_dir_done[k]}")

if __name__ == "__main__":
    main()

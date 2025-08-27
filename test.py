# ingest_to_os.py  (기존 ingest_to_es.py 대체)

import os
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from parse_xml import parse_darter_xml
import codecs

# ─────────────────────────────────────────────
# OpenSearch 접속 정보
# ─────────────────────────────────────────────
OS_HOSTS = os.getenv("OS_HOSTS", "http://localhost:9200").split(",")
OS_USER = os.getenv("OS_USER") or None
OS_PASS = os.getenv("OS_PASS") or None
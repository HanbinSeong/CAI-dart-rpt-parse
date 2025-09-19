from opensearchpy import OpenSearch
import requests
from dotenv import load_dotenv
import os

load_dotenv()


DART_API_KEY = os.getenv('DART_API_KEY')
url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"


# OpenSearch 연결 설정
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],  # OpenSearch 주소
    http_auth=("admin", "admin"),                 # 보안 계정 (있다면)
    use_ssl=False,
    verify_certs=False
)


new_mapping = {
    "properties": {
        "financials": {
            "properties": {
                # 절대값
                "revenue": { "type": "double" },             # 매출액
                "operating_income": { "type": "double" },    # 영업이익
                "net_income": { "type": "double" },          # 당기순이익
                "total_assets": { "type": "double" },        # 총자산
                "equity": { "type": "double" },              # 자본총계
                "cash_flow_operating": { "type": "double" }, # 영업활동현금흐름
                "cash_flow_investing": { "type": "double" }, # 투자활동현금흐름
                "cash_flow_financing": { "type": "double" }, # 재무활동현금흐름

                # 파생 지표
                "operating_margin": { "type": "double" },      # 영업이익률
                "net_margin": { "type": "double" },            # 순이익률
                "debt_to_equity": { "type": "double" },        # 부채비율
                "equity_ratio": { "type": "double" },          # 자기자본비율
                "operating_cf_to_investing_cf": { "type": "double" }, # 영업CF / 투자CF
                "operating_cf_to_revenue": { "type": "double" },      # 영업CF / 매출액
                "revenue_growth": { "type": "double" },        # 매출 증가율
                "operating_income_growth": { "type": "double" } # 영업이익 증가율
            }
        }
    }
}


ACCOUNT_MAP = {
    "revenue": ["수익(매출액)", "매출액", "매출", "영업수익"],
    "operating_income": ["영업이익", "영업손실", "영업이익(손실)"],
    "net_income": ["당기순이익", "당기순이익(손실)", "지배기업의 소유주에게 귀속되는 당기순이익(손실)", ],
    "total_assets": ["자산총계"],
    "equity": ["자본총계"],
    "cash_flow_operating": ["영업활동현금흐름", "영업활동순현금흐름"],
    "cash_flow_investing": ["투자활동현금흐름", "투자활동순현금흐름"],
    "cash_flow_financing": ["재무활동현금흐름", "재무활동순현금흐름"]
}

STATEMENT_MAP = {
    "revenue": ["CIS"],
    "operating_income": ["CIS"],
    "net_income": ["CIS"],   # ← 여기서만 CIS 기준으로 제한
    "total_assets": ["BS"],
    "equity": ["BS"],
    "cash_flow_operating": ["CF"],
    "cash_flow_investing": ["CF"],
    "cash_flow_financing": ["CF"]
}

def get_all_corp_codes(index="rpt_sec_eq"):
    """인덱스에서 고유한 corp_code 목록 가져오기"""
    result = client.search(
        index=index,
        body={
            "size": 0,
            "aggs": {
                "unique_corp_codes": {
                    "terms": {"field": "corp_code", "size": 10000}
                }
            }
        }
    )
    return [b["key"] for b in result["aggregations"]["unique_corp_codes"]["buckets"]]


def calculate_growth(current, previous):
    if previous in [0.0, None]:
        return None
    return (current - previous) / previous


def parse_amount(value, account_name):
    num = float(str(value).replace(",", "").strip() or 0)
    if "손실" in account_name:  # 손실은 음수 처리
        num = -abs(num)
    return num


def fetch_financial_data(corp_code, year="2024", reprt_code="11011"):
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": reprt_code,
        "fs_div": "CFS"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        result = response.json()

        if result.get("status") != "000":
            params["fs_div"] = "OFS"
            response = requests.get(url, params=params, timeout=10)
            result = response.json()

        data_list = result.get("list", [])

        fin = {k: 0.0 for k in ACCOUNT_MAP.keys()}
        fin.update({"revenue_growth": None, "operating_income_growth": None})
        prev_values = {}

        for item in data_list:
            account_name = item.get("account_nm", "").strip()
            sj_div = item.get("sj_div", "").strip()
            thstrm = parse_amount(item.get("thstrm_amount", "0"), account_name)
            frmtrm = parse_amount(item.get("frmtrm_amount", "0"), account_name)

            for field, aliases in ACCOUNT_MAP.items():
                if account_name in aliases:
                    # 올바른 재무제표(sj_div)인지 체크
                    allowed_statements = STATEMENT_MAP.get(field, [])
                    if allowed_statements and sj_div not in allowed_statements:
                        continue  # 스킵

                    # 값이 이미 있고 새로운 값이 0이면 무시 (덮어쓰기 방지)
                    if fin[field] and thstrm == 0:
                        continue

                    fin[field] = thstrm
                    if field in ["revenue", "operating_income"]:
                        prev_values[field] = frmtrm

        # 파생 지표
        fin["operating_margin"] = fin["operating_income"] / fin["revenue"] if fin["revenue"] else 0.0
        fin["net_margin"] = fin["net_income"] / fin["revenue"] if fin["revenue"] else 0.0
        fin["debt_to_equity"] = ((fin["total_assets"] - fin["equity"]) / fin["equity"]) if fin["equity"] else 0.0
        fin["equity_ratio"] = fin["equity"] / fin["total_assets"] if fin["total_assets"] else 0.0
        fin["operating_cf_to_investing_cf"] = (fin["cash_flow_operating"] / fin["cash_flow_investing"]) if fin["cash_flow_investing"] else 0.0
        fin["operating_cf_to_revenue"] = (fin["cash_flow_operating"] / fin["revenue"]) if fin["revenue"] else 0.0

        # 증가율
        fin["revenue_growth"] = calculate_growth(fin["revenue"], prev_values.get("revenue"))
        fin["operating_income_growth"] = calculate_growth(fin["operating_income"], prev_values.get("operating_income"))

        return fin

    except Exception as e:
        print(f"Exception fetching data for {corp_code}: {e}")
        return None


def update_financials_to_os(index="rpt_sec_eq"):
    res = client.indices.put_mapping(
        index="rpt_sec_eq",
        body=new_mapping
    )
    
    if res['acknowledged']:
        corp_codes = get_all_corp_codes(index)

        for corp_code in corp_codes:
            fin_data = fetch_financial_data(corp_code)
            if not fin_data:
                print(f"No financial data found for corp_code={corp_code}")
                continue

            try:
                client.update_by_query(
                    index=index,
                    body={
                        "script": {
                            "source": "ctx._source.financials = params.fin",
                            "lang": "painless",
                            "params": {"fin": fin_data}
                        },
                        "query": {"term": {"corp_code": corp_code}}
                    }
                )
                print(f"Updated financials for corp_code={corp_code}")
            except Exception as e:
                print(f"Error updating corp_code={corp_code}: {e}")



if __name__ == "__main__":
    update_financials_to_os()
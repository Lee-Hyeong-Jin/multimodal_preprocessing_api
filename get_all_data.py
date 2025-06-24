from opensearchpy import OpenSearch
import pandas as pd

client = OpenSearch(
    hosts=["http://localhost:9200"],
    http_auth=("admin", "Happybit25@!"),
    use_ssl=True,
    verify_certs=False  # 개발 중엔 False, 운영에서는 TLS 인증서 확인을 위해 True 권장
)

INDEX = "029_multimodal_manual_20250624"
# Scroll 방식으로 전체 문서 가져오기
def fetch_all_documents(index):
    all_docs = []
    scroll_time = '2m'
    batch_size = 1000

    response = client.search(
        index=index,
        scroll=scroll_time,
        size=batch_size,
        body={"query": {"match_all": {}}}
    )

    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    all_docs.extend(hits)

    while len(hits) > 0:
        response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        all_docs.extend(hits)

    return all_docs

# 변환: _source만 추출하여 DataFrame으로
docs = fetch_all_documents(INDEX)
df = pd.DataFrame([doc['_source'] for doc in docs])
print(df.iloc[42])

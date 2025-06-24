import requests

def embedding(text: str):
    url = "http://10.240.252.62:10105/v1/embeddings"
    headers = {
        "Content-Tpye": "application/json"
    }
    data = {
      "input": text,
      "model": "text-embedding-3-large",
      "encoding_format": "float"
    }
    
    response = requests.post(url, json=data, headers=headers)
    return response.json()['data'][0]['embedding']

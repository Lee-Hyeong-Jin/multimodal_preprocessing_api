from typing import Any, Dict, List, Optional
from langchain_text_splitters.character import RecursiveCharacterTextSplitter


class RecursiveStrategy:
    DEFAULT_SEPARATORS = ["\n\n", "\n", " ", ""]

    def chunking(
        self,
        text: str,
        chunk_size: int = 500,
        overlap: int = 50,
        separators: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        단일 문자열 입력을 Recursive 방식으로 청킹합니다.

        :param text: 입력 텍스트 문자열
        :param chunk_size: 각 청크의 최대 길이
        :param overlap: 청크 간 오버랩 길이
        :param separators: 텍스트 분할 기준
        :return: 청크 리스트 [{Chunk_id, Content}]
        """

        if separators is None:
            separators = self.DEFAULT_SEPARATORS

        if not text.strip():
            raise ValueError("[RecursiveStrategy] 입력 텍스트가 비어 있습니다.")

        if chunk_size <= 0:
            raise ValueError("[RecursiveStrategy] chunk_size는 양수여야 합니다.")

        if overlap < 0 or overlap >= chunk_size:
            raise ValueError("[RecursiveStrategy] overlap은 0 이상이고 chunk_size보다 작아야 합니다.")

        # LangChain Splitter 초기화
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=overlap,
            separators=separators
        )

        # 텍스트 청킹
        split_texts = splitter.split_text(text)
        return [
            {"Chunk_id": i + 1, "Content": chunk}
            for i, chunk in enumerate(split_texts)
        ]

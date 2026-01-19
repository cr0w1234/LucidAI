from abc import ABC, abstractmethod
from typing import List


class BaseRetriever(ABC):
    @abstractmethod
    def retrieve(self, query: str) -> List[str]:
        raise NotImplementedError("Subclasses must implement BaseRetriever.retrieve method.")


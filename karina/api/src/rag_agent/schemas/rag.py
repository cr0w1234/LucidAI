from pydantic import BaseModel, Field, field_validator

class RAGQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, description="User's question")

    @field_validator("query") #tells Pydantic when building a RAGQueryRequest model, run this function to validate the query field's value
    @classmethod #required by Pydantic
    def validate_query_nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Query cannot be empty")
        return value


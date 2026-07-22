from pydantic import BaseModel, Field, field_validator


class Book(BaseModel):
    title: str = Field(min_length=1)
    author: str = Field(min_length=1)
    category: str = Field(min_length=1)
    pages: int = Field(gt=0)
    publisher: str = Field(min_length=1)

    @field_validator("title", "author", "category", "publisher")
    @classmethod
    def clean_text(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Value cannot be empty")

        return value
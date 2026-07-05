from pydantic import BaseModel


class Message(BaseModel):
    id: int
    produto: int
    modalidade: int

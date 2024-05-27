from pydantic import BaseModel


class Config:
    protected_namespaces = ()


class AltimateBaseModel(BaseModel):
    model_unique_id: str

    class Config:
        protected_namespaces = ()

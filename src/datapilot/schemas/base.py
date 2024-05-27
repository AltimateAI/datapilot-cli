from pydantic import BaseModel
from pydantic import ConfigDict


class AltimateBaseModel(BaseModel):
    class Config:
        model_config = ConfigDict(protected_namespaces=())

from pydantic import BaseModel


class LivenessCommand(BaseModel):
    pass


class ReadinessCommand(BaseModel):
    pass

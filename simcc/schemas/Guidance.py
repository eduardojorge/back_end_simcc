from pydantic import BaseModel


class GuidanceMetrics(BaseModel):
    year: int
    m_completed: int
    m_in_progress: int
    ic_completed: int
    ic_in_progress: int
    d_completed: int
    d_in_progress: int
    g_completed: int

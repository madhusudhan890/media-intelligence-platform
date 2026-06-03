from pydantic import BaseModel
from typing import List

class AudioChunkMessage(BaseModel):
    roomId: str
    peerId: str
    peerName: str = "Anonymous"
    chunkId: str
    timestamp: str
    durationMs: int
    audioData: str

class TranscriptMessage(BaseModel):
    roomId: str
    peerId: str
    peerName: str = "Anonymous"
    chunkId: str
    text: str
    confidence: float
    timestamp: str

class DecisionItem(BaseModel):
    text: str

class ActionItem(BaseModel):
    owner: str
    task: str

class DeadlineItem(BaseModel):
    text: str

class RiskItem(BaseModel):
    text: str

class AIInsightMessage(BaseModel):
    roomId: str
    summary: str
    topics: List[str]
    decisions: List[DecisionItem]
    action_items: List[ActionItem]
    deadlines: List[DeadlineItem]
    risks: List[RiskItem]
    timestamp: str

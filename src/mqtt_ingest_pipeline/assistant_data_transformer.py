import logging
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class CommsBridgeData(SQLModel, table=True):
    time: datetime = Field(primary_key=True)
    message_id: uuid.UUID = Field(primary_key=True)
    room: str = Field(index=True)
    topic: str
    payload: dict[str, Any] = Field(sa_type=JSONB)


def transform_comms_bridge_message(
    topic: str, payload: dict[str, Any], logger: logging.Logger
) -> CommsBridgeData | None:
    parts = topic.strip("/").split("/")
    if len(parts) != 5:
        logger.warning("Topic not matching expected length: %s", topic)
        return None
    try:
        comms_bridge_date = CommsBridgeData(
            time=datetime.now(),
            message_id=payload.get("id"),
            room=payload.get("room"),
            topic=topic,
            payload=payload,
        )
        CommsBridgeData.model_validate(comms_bridge_date.model_dump())
        return comms_bridge_date
    except Exception as e:
        logger.error("Error storing message for topic %s: %s", topic, str(e))
    return None

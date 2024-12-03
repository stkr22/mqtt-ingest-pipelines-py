import asyncio

import typer
from aiomqtt import Client
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel, text

from mqtt_ingest_pipeline import assistant_data_transformer, iot_data_transformer, mqtt_data_pipeline, utility

app = typer.Typer()


@app.command()
def main() -> None:
    asyncio.run(start_pipeline())


async def start_pipeline() -> None:
    mqtt_host, mqtt_port = utility.get_mqtt_config()
    db_url = utility.get_db_url()
    client = Client(hostname=mqtt_host, port=mqtt_port)
    db_engine_async = create_async_engine(db_url)
    async with db_engine_async.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
        await conn.execute(
            text("""
            SELECT create_hypertable('iotdata', 'time',
                if_not_exists => TRUE
            );
            """)
        )
        await conn.execute(
            text("""
            SELECT create_hypertable('commsbridgedata', 'time',
                if_not_exists => TRUE
            );
            """)
        )

    async with client as mqtt_client, asyncio.TaskGroup() as tg:
        pipeline = mqtt_data_pipeline.MQTTDataPipeline(
            mqtt_client=mqtt_client,
            db_engine=db_engine_async,
            task_group=tg,
            logger=utility.CustomLogger.get_logger("MQTTDataPipeline"),
        )
        pipeline.register_transformer("zigbee2mqtt/+/+/+", iot_data_transformer.transform_iot_message)
        pipeline.register_transformer(
            "assistant/comms_bridge/all/+/input", assistant_data_transformer.transform_comms_bridge_message
        )
        await pipeline.setup_mqtt_subscriptions()
        await pipeline.listen_to_messages(client)


if __name__ == "__main__":
    typer.run(main)

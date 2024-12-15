import asyncio
from typing import Annotated

import typer
from aiomqtt import Client
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel, text

from mqtt_ingest_pipeline import assistant_data_transformer, iot_data_transformer, mqtt_data_pipeline, utility

app = typer.Typer()


async def setup_database(engine: AsyncEngine) -> None:
    """Initialize database tables and hypertables."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
        # Create hypertables
        for table in ["iotdata", "commsbridgedata"]:
            await conn.execute(
                text(f"""
                SELECT create_hypertable('{table}', 'time',
                    if_not_exists => TRUE
                );
                """)
            )


async def start_pipeline(
    mqtt_host: str,
    mqtt_port: int,
    db_url: str,
) -> None:
    """Initialize and run the MQTT data pipeline."""
    client = Client(hostname=mqtt_host, port=mqtt_port)
    db_engine_async = create_async_engine(db_url)

    # Setup database
    await setup_database(db_engine_async)

    # Initialize and run pipeline
    async with client as mqtt_client, asyncio.TaskGroup() as tg:
        pipeline = mqtt_data_pipeline.MQTTDataPipeline(
            mqtt_client=mqtt_client,
            db_engine=db_engine_async,
            task_group=tg,
            logger=utility.CustomLogger.get_logger("MQTTDataPipeline"),
        )

        # Register transformers
        pipeline.register_transformer("zigbee2mqtt/+/+/+", iot_data_transformer.transform_iot_message)
        pipeline.register_transformer(
            "assistant/comms_bridge/all/+/input", assistant_data_transformer.transform_comms_bridge_message
        )

        await pipeline.setup_mqtt_subscriptions()
        await pipeline.listen_to_messages(client)


@app.command()
def main(
    mqtt_host: Annotated[
        str,
        typer.Option(
            envvar="MQTT_HOST",
            help="MQTT broker hostname",
        ),
    ] = "localhost",
    mqtt_port: Annotated[
        int,
        typer.Option(
            envvar="MQTT_PORT",
            help="MQTT broker port",
        ),
    ] = 1883,
    db_user: Annotated[
        str,
        typer.Option(
            envvar="POSTGRES_USER",
            help="PostgreSQL username",
        ),
    ] = "postgres",
    db_password: Annotated[
        str,
        typer.Option(
            envvar="POSTGRES_PASSWORD",
            help="PostgreSQL password",
            prompt=True,
            hide_input=True,
        ),
    ] = "postgres",
    db_host: Annotated[
        str,
        typer.Option(
            envvar="POSTGRES_HOST",
            help="PostgreSQL hostname",
        ),
    ] = "localhost",
    db_port: Annotated[
        int,
        typer.Option(
            envvar="POSTGRES_PORT",
            help="PostgreSQL port",
        ),
    ] = 5432,
    db_name: Annotated[
        str,
        typer.Option(
            envvar="POSTGRES_DB",
            help="PostgreSQL database name",
        ),
    ] = "postgres",
) -> None:
    """Run the MQTT to TimescaleDB pipeline."""
    db_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    asyncio.run(start_pipeline(mqtt_host, mqtt_port, db_url))


if __name__ == "__main__":
    app()

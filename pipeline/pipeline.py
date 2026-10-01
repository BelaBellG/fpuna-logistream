import json
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows


# ============================================================
# CONFIGURACIÓN
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

EXPANSION_SERVICE = os.getenv(
    "EXPANSION_SERVICE",
    "expansion:45193",
)

INPUT_TOPIC = "logistics.events.v1"
OUTPUT_TOPIC = "logistics.metrics.v1"
INVALID_TOPIC = "logistics.invalid.v1"

WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 120


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def parse_event_time(value: str) -> float:
    """
    Convierte un timestamp ISO 8601 a Unix timestamp.
    """

    normalized = value.replace("Z", "+00:00")

    dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.timestamp()


def decode_event(element):
    """
    Convierte el mensaje recibido desde Kafka en un diccionario.

    Kafka entrega:
        (key, value)

    El value contiene el JSON del evento.
    """

    key, value = element

    try:
        event = json.loads(
            value.decode("utf-8")
        )

        if "event_id" not in event:
            raise ValueError(
                "Falta event_id"
            )

        if "event_time" not in event:
            raise ValueError(
                "Falta event_time"
            )

        if "payload" not in event:
            raise ValueError(
                "Falta payload"
            )

        return event

    except Exception as exc:

        return {
            "_invalid": True,
            "error": str(exc),
            "raw": value.decode(
                "utf-8",
                errors="replace",
            ),
        }


# ============================================================
# VALIDACIÓN
# ============================================================

class ValidateEvent(beam.DoFn):
    """
    Valida la estructura mínima de cada evento.
    """

    REQUIRED_PAYLOAD_FIELDS = [
        "vehicle_id",
        "route_id",
        "package_id",
        "status",
        "delay_minutes",
    ]

    def process(self, event):

        if event.get("_invalid"):

            yield beam.pvalue.TaggedOutput(
                "invalid",
                event,
            )

            return

        payload = event.get(
            "payload",
            {},
        )

        missing = [
            field
            for field in self.REQUIRED_PAYLOAD_FIELDS
            if field not in payload
        ]

        if missing:

            event["_invalid"] = True

            event["error"] = (
                "Campos faltantes: "
                + ", ".join(missing)
            )

            yield beam.pvalue.TaggedOutput(
                "invalid",
                event,
            )

            return

        try:

            parse_event_time(
                event["event_time"]
            )

        except Exception as exc:

            event["_invalid"] = True

            event["error"] = (
                f"event_time inválido: {exc}"
            )

            yield beam.pvalue.TaggedOutput(
                "invalid",
                event,
            )

            return

        yield event


# ============================================================
# ASIGNACIÓN DE EVENT TIME
# ============================================================

class AssignEventTimestamp(beam.DoFn):
    """
    Asigna event_time como timestamp lógico de Beam.
    """

    def process(self, event):

        if event.get("_invalid"):
            return

        timestamp = parse_event_time(
            event["event_time"]
        )

        yield beam.window.TimestampedValue(
            event,
            timestamp,
        )


# ============================================================
# DEDUPLICACIÓN
# ============================================================

class DeduplicateEvents(beam.DoFn):
    """
    Deduplicación por event_id.

    Para la demostración local se mantiene el conjunto
    de identificadores procesados durante la ejecución
    del worker.

    Un mismo event_id no debe participar dos veces
    en la agregación.
    """

    def setup(self):

        self.seen = set()

    def process(self, event):

        if event.get("_invalid"):
            return

        event_id = event["event_id"]

        if event_id in self.seen:
            return

        self.seen.add(event_id)

        yield event


# ============================================================
# FORMATEO DE MÉTRICAS
# ============================================================

class FormatMetric(beam.DoFn):
    """
    Genera una métrica por ruta y ventana.
    """

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
    ):

        route_id, values = element

        values = list(values)

        if not values:
            return

        event_count = len(values)

        total_delay = sum(
            float(
                item["payload"]["delay_minutes"]
            )
            for item in values
        )

        average_delay = (
            total_delay / event_count
        )

        event_ids = sorted(
            item["event_id"]
            for item in values
        )

        window_start = (
            window.start
            .to_utc_datetime()
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

        window_end = (
            window.end
            .to_utc_datetime()
            .isoformat()
            .replace(
                "+00:00",
                "Z",
            )
        )

        # ID determinístico de la métrica.
        #
        # La misma ruta + ventana produce siempre
        # el mismo identificador.

        metric_id = (
            f"{route_id}:"
            f"{int(window.start.micros / 1_000_000)}"
        )

        yield {
            "metric_id": metric_id,
            "window_start": window_start,
            "window_end": window_end,
            "route_id": route_id,
            "event_count": event_count,
            "average_delay_minutes": round(
                average_delay,
                2,
            ),
            "event_ids": event_ids,
        }


# ============================================================
# SERIALIZACIÓN PARA KAFKA
# ============================================================

def to_kafka_record(event):

    key = event[
        "route_id"
    ].encode("utf-8")

    value = json.dumps(
        event,
        ensure_ascii=False,
    ).encode("utf-8")

    return key, value


def invalid_to_kafka_record(event):

    key = event.get(
        "event_id",
        "invalid",
    ).encode("utf-8")

    value = json.dumps(
        event,
        ensure_ascii=False,
    ).encode("utf-8")

    return key, value


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def build_pipeline():

    options = PipelineOptions([
        "--runner=DirectRunner",
        "--streaming",
        "--environment_type=LOOPBACK",
        "--direct_running_mode=in_memory",
        "--save_main_session",
    ])

    with beam.Pipeline(
        options=options
    ) as pipeline:

        # ====================================================
        # 1. LECTURA DESDE KAFKA
        # ====================================================

        kafka_messages = (
            pipeline
            | "ReadFromKafka"
            >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers":
                        KAFKA_BOOTSTRAP_SERVERS,

                    "group.id":
                        "logistream-beam",

                    "auto.offset.reset":
                        "earliest",
                },

                topics=[
                    INPUT_TOPIC
                ],

                expansion_service=
                    EXPANSION_SERVICE,
            )
        )

        # ====================================================
        # 2. DECODIFICACIÓN JSON
        # ====================================================

        decoded = (
            kafka_messages
            | "DecodeJSON"
            >> beam.Map(
                decode_event
            )
        )

        # ====================================================
        # 3. VALIDACIÓN
        # ====================================================

        validated = (
            decoded
            | "ValidateEvents"
            >> beam.ParDo(
                ValidateEvent()
            ).with_outputs(
                "invalid",
                main="valid",
            )
        )

        valid_events = validated.valid

        invalid_events = validated.invalid

        # ====================================================
        # 4. ASIGNACIÓN DEL TIEMPO DE EVENTO
        # ====================================================

        timestamped = (
            valid_events
            | "AssignEventTime"
            >> beam.ParDo(
                AssignEventTimestamp()
            )
        )

        # ====================================================
        # 5. DEDUPLICACIÓN
        # ====================================================

        deduplicated = (
            timestamped
            | "DeduplicateEvents"
            >> beam.ParDo(
                DeduplicateEvents()
            )
        )

        # ====================================================
        # 6. VENTANA TEMPORAL
        # ====================================================
        #
        # Ventana:
        #   fija de 60 segundos.
        #
        # Tiempo:
        #   event_time.
        #
        # Early trigger:
        #   cada 10 segundos de tiempo de procesamiento.
        #
        # On-time:
        #   cuando el watermark supera el final
        #   de la ventana.
        #
        # Late trigger:
        #   cada elemento tardío.
        #
        # Lateness:
        #   120 segundos.
        #
        # Acumulación:
        #   ACCUMULATING.
        # ====================================================

        windowed = (
            deduplicated
            | "FixedWindows"
            >> beam.WindowInto(

                FixedWindows(
                    WINDOW_SECONDS
                ),

                trigger=trigger.AfterWatermark(

                    early=trigger.AfterProcessingTime(
                        10
                    ),

                    late=trigger.AfterCount(
                        1
                    ),
                ),

                accumulation_mode=(
                    trigger.AccumulationMode.ACCUMULATING
                ),

                allowed_lateness=(
                    ALLOWED_LATENESS_SECONDS
                ),
            )
        )

        # ====================================================
        # 7. CLAVE POR RUTA
        # ====================================================

        keyed_by_route = (
            windowed
            | "KeyByRoute"
            >> beam.Map(
                lambda event: (
                    event[
                        "payload"
                    ][
                        "route_id"
                    ],
                    event,
                )
            )
        )

        # ====================================================
        # 8. AGRUPACIÓN
        # ====================================================

        grouped = (
            keyed_by_route
            | "GroupByRoute"
            >> beam.GroupByKey()
        )

        # ====================================================
        # 9. AGREGACIÓN
        # ====================================================

        metrics = (
            grouped
            | "BuildMetrics"
            >> beam.ParDo(
                FormatMetric()
            )
        )

        # ====================================================
        # 10. SALIDA DE MÉTRICAS A KAFKA
        # ====================================================

        (
            metrics
            | "SerializeMetrics"
            >> beam.Map(
                to_kafka_record
            )

            | "WriteMetricsToKafka"
            >> WriteToKafka(

                producer_config={
                    "bootstrap.servers":
                        KAFKA_BOOTSTRAP_SERVERS,

                    # Productor Kafka idempotente.
                    "enable.idempotence":
                        "true",

                    "acks":
                        "all",
                },

                topic=OUTPUT_TOPIC,

                expansion_service=
                    EXPANSION_SERVICE,
            )
        )

        # ====================================================
        # 11. SALIDA DE EVENTOS INVÁLIDOS
        # ====================================================

        (
            invalid_events
            | "SerializeInvalidEvents"
            >> beam.Map(
                invalid_to_kafka_record
            )

            | "WriteInvalidEvents"
            >> WriteToKafka(

                producer_config={
                    "bootstrap.servers":
                        KAFKA_BOOTSTRAP_SERVERS,

                    "enable.idempotence":
                        "true",

                    "acks":
                        "all",
                },

                topic=INVALID_TOPIC,

                expansion_service=
                    EXPANSION_SERVICE,
            )
        )


# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    build_pipeline()
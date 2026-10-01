import json
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


# ============================================================
# CONFIGURACIÓN
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC = "logistics.events.v1"


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def iso(dt: datetime) -> str:
    """
    Convertir datetime a formato ISO 8601 UTC.
    Ejemplo:
    2026-10-01T14:03:20Z
    """
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_event(
    event_id: str,
    route_id: str,
    vehicle_id: str,
    package_id: str,
    event_time: datetime,
    delay_minutes: int,
    status: str = "EN_TRANSITO",
) -> dict:
    """
    Construir un evento logístico respetando el contrato
    definido para LogiStream.
    """

    return {
        "event_id": event_id,
        "key": route_id,
        "event_time": iso(event_time),
        "schema_version": 1,
        "payload": {
            "vehicle_id": vehicle_id,
            "route_id": route_id,
            "package_id": package_id,
            "status": status,
            "delay_minutes": delay_minutes,
        },
    }


def send_event(producer: KafkaProducer, event: dict) -> None:
    """
    Enviar un evento a Kafka.

    La clave Kafka será route_id.
    El value será el objeto JSON completo.
    """

    route_id = event["payload"]["route_id"]

    producer.send(
        TOPIC,
        key=route_id,
        value=event,
    )

    producer.flush()

    print(
        f"Enviado | "
        f"event_id={event['event_id']} | "
        f"route={route_id} | "
        f"event_time={event['event_time']}"
    )


# ============================================================
# PRODUCTOR PRINCIPAL
# ============================================================

def main() -> None:

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,

        # Kafka convierte automáticamente la clave
        # de texto a bytes.
        key_serializer=lambda value: value.encode("utf-8"),

        # Kafka convierte el diccionario a JSON.
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    # --------------------------------------------------------
    # Tiempo base para todos los eventos
    # --------------------------------------------------------

    base_time = datetime.now(timezone.utc).replace(microsecond=0)

    # ========================================================
    # DEFINICIÓN DE EVENTOS
    # ========================================================

    events = [

        # ----------------------------------------------------
        # 1. EVENTO NORMAL
        # ----------------------------------------------------

        build_event(
            event_id="evt-0001",
            route_id="RUTA-01",
            vehicle_id="VEH-001",
            package_id="PKG-1001",
            event_time=base_time,
            delay_minutes=2,
        ),

        # ----------------------------------------------------
        # 2. EVENTO NORMAL
        # ----------------------------------------------------

        build_event(
            event_id="evt-0002",
            route_id="RUTA-02",
            vehicle_id="VEH-002",
            package_id="PKG-1002",
            event_time=base_time + timedelta(seconds=10),
            delay_minutes=5,
        ),

        # ----------------------------------------------------
        # 3. EVENTO QUE SERÁ DUPLICADO
        # ----------------------------------------------------

        build_event(
            event_id="evt-0003",
            route_id="RUTA-01",
            vehicle_id="VEH-001",
            package_id="PKG-1003",
            event_time=base_time + timedelta(seconds=20),
            delay_minutes=3,
        ),

        # ----------------------------------------------------
        # 4. EVENTO FUERA DE ORDEN
        #
        # Tiene event_time = +50 segundos.
        # Se enviará antes que evt-0005.
        # ----------------------------------------------------

        build_event(
            event_id="evt-0004",
            route_id="RUTA-01",
            vehicle_id="VEH-001",
            package_id="PKG-1004",
            event_time=base_time + timedelta(seconds=50),
            delay_minutes=8,
        ),

        # ----------------------------------------------------
        # 5. EVENTO FUERA DE ORDEN
        #
        # Tiene event_time = +30 segundos.
        # Se envía DESPUÉS de evt-0004.
        #
        # Esto permite demostrar que el orden de llegada
        # no coincide con event_time.
        # ----------------------------------------------------

        build_event(
            event_id="evt-0005",
            route_id="RUTA-01",
            vehicle_id="VEH-001",
            package_id="PKG-1005",
            event_time=base_time + timedelta(seconds=30),
            delay_minutes=12,
        ),

        # ----------------------------------------------------
        # 6. EVENTO TARDÍO
        #
        # Su event_time es anterior al momento actual.
        # Beam decidirá posteriormente si se encuentra
        # dentro de allowed_lateness.
        # ----------------------------------------------------

        build_event(
            event_id="evt-0006",
            route_id="RUTA-02",
            vehicle_id="VEH-002",
            package_id="PKG-1006",
            event_time=base_time - timedelta(seconds=30),
            delay_minutes=15,
        ),

        # ----------------------------------------------------
        # 7. EVENTO MUY TARDÍO
        #
        # Se encuentra 5 minutos antes del tiempo base.
        #
        # Con allowed_lateness = 120 segundos,
        # Beam podrá descartarlo si el watermark ya avanzó
        # más allá del límite permitido.
        # ----------------------------------------------------

        build_event(
            event_id="evt-0007",
            route_id="RUTA-03",
            vehicle_id="VEH-003",
            package_id="PKG-1007",
            event_time=base_time - timedelta(seconds=300),
            delay_minutes=20,
        ),
    ]

    # ========================================================
    # ENVÍO DE EVENTOS
    # ========================================================

    print("=" * 70)
    print("LogiStream - Productor de eventos")
    print("=" * 70)
    print(f"Kafka: {BOOTSTRAP_SERVERS}")
    print(f"Tópico: {TOPIC}")
    print()

    # --------------------------------------------------------
    # Evento normal
    # --------------------------------------------------------

    send_event(producer, events[0])
    time.sleep(0.5)

    # --------------------------------------------------------
    # Segundo evento normal
    # --------------------------------------------------------

    send_event(producer, events[1])
    time.sleep(0.5)

    # --------------------------------------------------------
    # Evento que posteriormente será duplicado
    # --------------------------------------------------------

    send_event(producer, events[2])
    time.sleep(0.5)

    # ========================================================
    # DUPLICADO
    #
    # Se envía exactamente el mismo event_id nuevamente.
    # Beam deberá deduplicarlo.
    # ========================================================

    print("\n>>> Enviando DUPLICADO: evt-0003")

    send_event(producer, events[2])
    time.sleep(0.5)

    # ========================================================
    # FUERA DE ORDEN
    #
    # evt-0004 tiene event_time +50 segundos.
    # evt-0005 tiene event_time +30 segundos.
    #
    # Primero enviamos +50.
    # Después enviamos +30.
    #
    # Por lo tanto, el orden de llegada es diferente
    # al orden de event_time.
    # ========================================================

    print("\n>>> Enviando eventos FUERA DE ORDEN")

    send_event(producer, events[3])
    time.sleep(0.5)

    send_event(producer, events[4])
    time.sleep(0.5)

    # ========================================================
    # EVENTO TARDÍO
    # ========================================================

    print("\n>>> Enviando evento TARDÍO: evt-0006")

    send_event(producer, events[5])
    time.sleep(0.5)

    # ========================================================
    # EVENTO DEMASIADO TARDÍO
    # ========================================================

    print("\n>>> Enviando evento MUY TARDÍO: evt-0007")

    send_event(producer, events[6])

    # ========================================================
    # FINALIZACIÓN
    # ========================================================

    producer.flush()
    producer.close()

    print()
    print("=" * 70)
    print("Producción finalizada.")
    print("=" * 70)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
# LogiStream – Trabajo Práctico Final FPUNA

Sistema de procesamiento de eventos logísticos en tiempo real utilizando Apache Kafka y Apache Beam.

---

## 1. Descripción

LogiStream es una solución de procesamiento de eventos en streaming orientada al dominio de logística.

El sistema recibe eventos logísticos mediante Apache Kafka, los procesa utilizando Apache Beam y publica métricas agregadas nuevamente en Kafka.

El pipeline contempla:

- Validación de eventos.
- Separación de eventos inválidos.
- Procesamiento basado en Event Time.
- Deduplicación mediante `event_id`.
- Ventanas temporales.
- Manejo de eventos fuera de orden.
- Manejo de eventos tardíos.
- Agregación por ruta.
- Generación de métricas.
- Publicación de resultados en Kafka.

---

## 2. Arquitectura

La arquitectura general de LogiStream es:

```text
                         ┌──────────────────────┐
                         │   Productor Python   │
                         │   producer.py        │
                         └──────────┬───────────┘
                                    │
                                    │ Eventos JSON
                                    ▼
                         ┌──────────────────────┐
                         │      Apache Kafka    │
                         │                      │
                         │ logistics.events.v1  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │        Apache Beam           │
                    │                              │
                    │  • Lectura KafkaIO           │
                    │  • Decodificación JSON       │
                    │  • Validación                │
                    │  • Event Time                │
                    │  • Deduplicación             │
                    │  • Ventanas                  │
                    │  • Allowed Lateness          │
                    │  • Triggers                  │
                    │  • Agrupación por ruta       │
                    │  • Cálculo de métricas       │
                    └──────────────┬───────────────┘
                                   │
                         ┌─────────┴─────────┐
                         │                   │
                         ▼                   ▼
              ┌──────────────────┐  ┌────────────────────┐
              │ Eventos inválidos│  │ Métricas procesadas│
              │                  │  │                    │
              │ logistics.       │  │ logistics.         │
              │ invalid.v1       │  │ metrics.v1         │
              └──────────────────┘  └────────────────────┘
```

---

## 3. Tecnologías utilizadas

| Tecnología | Versión / Uso |
|---|---|
| Python | 3.12 |
| Apache Kafka | 4.0.1 |
| Apache Beam | 2.69.0 |
| Docker | Contenedores |
| Docker Compose | Orquestación |
| KafkaIO | Integración Apache Beam + Kafka |
| OpenJDK | 21 |

---

## 4. Estructura del proyecto

```text
FPUNA_LogiStream_TP_Final/
│
├── README.md
├── Documentacion_LogiStream_TP_Final.pdf
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .dockerignore
├── .gitignore
│
├── pipeline/
│   └── pipeline.py
│
├── producer/
│   └── producer.py
│
└── tests/
    └── ...
```

### Descripción de los principales archivos

**`docker-compose.yml`**

Define los servicios necesarios para ejecutar la solución:

- Apache Kafka.
- Apache Beam Expansion Service.
- Pipeline Apache Beam.

**`Dockerfile`**

Define la imagen utilizada para ejecutar el pipeline Python de Apache Beam, incluyendo las dependencias necesarias para la ejecución y comunicación con Docker.

**`pipeline/pipeline.py`**

Contiene la implementación principal del pipeline de procesamiento.

**`producer/producer.py`**

Genera y publica eventos de prueba en Kafka.

**`tests/`**

Contiene los archivos destinados a pruebas del proyecto.

**`Documentacion_LogiStream_TP_Final.pdf`**

Contiene la documentación técnica completa del proyecto.

---

## 5. Componentes de la solución

### 5.1 Apache Kafka

Kafka funciona como sistema de mensajería y transporte de eventos.

El entorno utiliza un único broker en modo KRaft.

#### Listener interno

```text
kafka:9092
```

Es utilizado por los servicios que se ejecutan dentro de Docker.

#### Listener externo

```text
localhost:9094
```

Es utilizado por el productor Python ejecutado desde el equipo local.

---

### 5.2 Apache Beam

Apache Beam implementa el procesamiento de los eventos.

El pipeline utiliza el `DirectRunner` para la ejecución local.

Entre las principales operaciones se encuentran:

1. Lectura de Kafka.
2. Conversión de bytes a JSON.
3. Validación.
4. Separación de eventos inválidos.
5. Asignación de Event Time.
6. Deduplicación.
7. Aplicación de ventanas.
8. Manejo de eventos tardíos.
9. Agrupación por ruta.
10. Cálculo de métricas.
11. Escritura de resultados en Kafka.

---

### 5.3 Expansion Service

El proyecto utiliza:

```text
apache/beam_java_expansion_service:2.69.0
```

El servicio permite utilizar los transforms Java necesarios para `KafkaIO` desde el pipeline desarrollado en Python.

El servicio se encuentra disponible en:

```text
45193
```

---

## 6. Tópicos Kafka

El sistema utiliza tres tópicos principales.

### 6.1 `logistics.events.v1`

Es el tópico de entrada.

Contiene los eventos logísticos generados por el productor.

### 6.2 `logistics.invalid.v1`

Es el tópico destinado a los eventos que no cumplen las validaciones establecidas.

Permite mantener una separación entre los eventos procesables y los eventos rechazados.

### 6.3 `logistics.metrics.v1`

Es el tópico de salida.

Contiene las métricas generadas por Apache Beam después del procesamiento y agregación de los eventos.

---

## 7. Contrato de eventos

Los eventos utilizan formato JSON.

La estructura esperada incluye:

```json
{
  "event_id": "evt-0001",
  "event_time": "2026-10-01T00:51:22Z",
  "payload": {
    "vehicle_id": "VEH-01",
    "route_id": "RUTA-01",
    "package_id": "PKG-001",
    "status": "DELIVERED",
    "delay_minutes": 5
  }
}
```

### Campos principales

| Campo | Descripción |
|---|---|
| `event_id` | Identificador único del evento |
| `event_time` | Fecha y hora del evento |
| `vehicle_id` | Identificador del vehículo |
| `route_id` | Identificador de la ruta |
| `package_id` | Identificador del paquete |
| `status` | Estado del evento |
| `delay_minutes` | Minutos de atraso |

---

## 8. Procesamiento de eventos

El pipeline procesa los eventos mediante las siguientes etapas.

### 8.1 Lectura

Apache Beam consume eventos desde:

```text
logistics.events.v1
```

### 8.2 Decodificación

El mensaje recibido desde Kafka es convertido desde bytes a una estructura JSON.

### 8.3 Validación

Se verifican los campos necesarios para que el evento pueda ser procesado.

Los eventos que no cumplen las condiciones establecidas son enviados al tópico:

```text
logistics.invalid.v1
```

### 8.4 Event Time

El pipeline utiliza el campo:

```text
event_time
```

como referencia temporal del evento.

Esto permite procesar eventos independientemente del orden en el que llegan físicamente al sistema.

### 8.5 Deduplicación

Los eventos utilizan:

```text
event_id
```

como identificador.

Cuando un mismo `event_id` aparece nuevamente, el evento duplicado no debe participar nuevamente en la agregación.

Ejemplo:

```text
evt-0003 → procesado
evt-0003 → duplicado → descartado
```

---

## 9. Ventanas temporales

El pipeline utiliza ventanas fijas de:

```text
60 segundos
```

La ventana se determina utilizando `event_time`.

Además, se configura:

```text
Allowed Lateness: 120 segundos
```

Esto permite aceptar eventos que llegan con retraso dentro del límite establecido.

---

## 10. Triggers

El procesamiento utiliza diferentes condiciones de disparo.

### Early Trigger

Permite generar resultados parciales durante el procesamiento.

Configuración:

```text
Cada 10 segundos
```

### On-Time Trigger

Se produce cuando el watermark indica que la ventana puede considerarse procesada en tiempo.

### Late Trigger

Permite reaccionar ante la llegada de eventos tardíos aceptados dentro del límite de `Allowed Lateness`.

---

## 11. Agregación

Los eventos son agrupados utilizando:

```text
route_id
```

La finalidad es obtener métricas correspondientes a cada ruta dentro de cada ventana temporal.

Entre los valores generados se encuentran:

- Cantidad de eventos.
- Promedio de `delay_minutes`.
- Identificadores de los eventos procesados.
- Inicio y fin de la ventana.
- Identificador de la métrica.

---

## 12. Ejemplo de métrica

Una métrica generada puede presentar una estructura similar a:

```json
{
  "metric_id": "RUTA-01:...",
  "window_start": "2026-10-01T00:51:00Z",
  "window_end": "2026-10-01T00:52:00Z",
  "route_id": "RUTA-01",
  "event_count": 3,
  "average_delay_minutes": 4.33,
  "event_ids": [
    "evt-0001",
    "evt-0003",
    "evt-0005"
  ]
}
```

---

## 13. Escenarios de prueba

El productor incluye diferentes situaciones para comprobar el comportamiento del sistema.

### Evento normal

```text
evt-0001
evt-0002
evt-0003
```

Permite verificar el flujo normal de procesamiento.

### Evento duplicado

```text
evt-0003
```

Se envía nuevamente para comprobar la deduplicación.

### Eventos fuera de orden

```text
evt-0004
evt-0005
```

Los eventos poseen tiempos de evento diferentes al orden de llegada.

### Evento tardío

```text
evt-0006
```

Se utiliza para comprobar el comportamiento frente a eventos retrasados.

### Evento muy tardío

```text
evt-0007
```

Permite observar el comportamiento ante un evento cuyo atraso supera el escenario normal de procesamiento.

---

## 14. Requisitos previos

Para ejecutar el proyecto se necesita:

- Windows, Linux o macOS.
- Python 3.12.
- Docker Desktop.
- Docker Compose.
- Conexión a Internet para descargar las imágenes y dependencias.
- Entorno virtual Python para ejecutar el productor.

---

## 15. Instalación y ejecución

### Paso 1 – Construir la imagen

Desde la raíz del proyecto:

```powershell
docker compose build beam
```

### Paso 2 – Iniciar Kafka y Expansion Service

```powershell
docker compose up -d kafka expansion
```

Verificar:

```powershell
docker compose ps
```

Deberían encontrarse activos:

```text
logistream-kafka
logistream-expansion
```

### Paso 3 – Ejecutar el pipeline

```powershell
docker compose up beam
```

El pipeline comenzará a consumir eventos desde:

```text
logistics.events.v1
```

### Paso 4 – Ejecutar el productor

Abrir otra terminal en la raíz del proyecto y ejecutar:

```powershell
python producer\producer.py
```

El productor enviará los eventos de prueba a:

```text
localhost:9094
```

y al tópico:

```text
logistics.events.v1
```

---

## 16. Verificación de tópicos

Para listar los tópicos existentes:

```powershell
docker exec logistream-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Deberían aparecer:

```text
__consumer_offsets
logistics.events.v1
logistics.invalid.v1
logistics.metrics.v1
```

---

## 17. Visualizar eventos de entrada

Para observar los eventos publicados:

```powershell
docker exec -it logistream-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logistics.events.v1 --from-beginning
```

---

## 18. Visualizar métricas

Para consultar las métricas producidas:

```powershell
docker exec -it logistream-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logistics.metrics.v1 --from-beginning
```

---

## 19. Visualizar eventos inválidos

Para consultar los eventos enviados al flujo de inválidos:

```powershell
docker exec -it logistream-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic logistics.invalid.v1 --from-beginning
```

---

## 20. Detener el entorno

Para detener todos los servicios:

```powershell
docker compose down
```

---

## 21. Comprobaciones realizadas

Durante el desarrollo fueron verificadas diferentes partes de la solución.

### Kafka

Se verificó:

- Inicio correcto del broker.
- Creación de los tópicos.
- Comunicación interna.
- Comunicación externa mediante `localhost:9094`.

### Productor

Se verificó la publicación de:

- Eventos normales.
- Eventos duplicados.
- Eventos fuera de orden.
- Eventos tardíos.
- Eventos muy tardíos.

### Pipeline

Se verificó la compilación de:

```text
pipeline/pipeline.py
```

mediante:

```powershell
docker compose run --rm beam python -m py_compile pipeline/pipeline.py
```

### Java

Se verificó la disponibilidad de Java dentro del contenedor:

```powershell
docker compose run --rm beam java -version
```

### Expansion Service

Se verificó el funcionamiento del servicio de expansión Apache Beam mediante el contenedor:

```text
logistream-expansion
```

utilizando el puerto:

```text
45193
```

### Docker

Se verificó la disponibilidad del cliente Docker dentro del contenedor Beam.

También se comprobó la comunicación con el daemon mediante:

```powershell
docker compose exec beam docker ps
```

y mediante la ejecución de:

```powershell
docker compose exec beam docker run --rm hello-world
```

---

## 22. Estado de la implementación

Los principales componentes de la solución fueron implementados y verificados individualmente:

- Apache Kafka.
- Productor Python.
- Tópico de entrada.
- Tópicos de salida.
- Apache Beam.
- KafkaIO.
- Expansion Service.
- Event Time.
- Deduplicación.
- Ventanas temporales.
- Allowed Lateness.
- Triggers.
- Agregación por ruta.
- Docker.
- Docker Compose.

Durante la integración final se presentó una incidencia relacionada con la inicialización del worker Java utilizado por Apache Beam/DirectRunner en el entorno local.

Se observó un error relacionado con el servicio de provisioning del worker, por lo que el procesamiento end-to-end no pudo completarse de forma estable durante la última prueba.

La incidencia se encuentra explicada con mayor detalle en la documentación técnica incluida en:

```text
Documentacion_LogiStream_TP_Final.pdf
```

---

## 23. Limitaciones conocidas

La ejecución local presentó una incidencia en la inicialización del worker Java de Apache Beam.

Esto impidió completar una validación end-to-end estable durante la última ejecución.

No obstante, se verificaron individualmente los principales componentes de la solución, incluyendo Kafka, productor, tópicos, Docker, Java, Expansion Service y la construcción del pipeline.

Para una siguiente iteración se recomienda:

- Revisar el entorno de workers de Apache Beam.
- Evaluar el runner utilizado para la ejecución.
- Automatizar pruebas de ventanas y Event Time.
- Implementar una estrategia de estado administrado para la deduplicación.
- Realizar una prueba end-to-end reproducible desde Kafka hasta el tópico de métricas.

---

## 24. Documentación técnica

La documentación técnica completa del proyecto se encuentra en:

```text
Documentacion_LogiStream_TP_Final.pdf
```

El documento contiene información adicional sobre:

- Arquitectura.
- Componentes.
- Contrato de eventos.
- Procesamiento.
- Event Time.
- Ventanas.
- Lateness.
- Triggers.
- Deduplicación.
- Métricas.
- Pruebas.
- Verificaciones.
- Incidencias.
- Limitaciones.
- Mejoras futuras.

---

## 25. Entrega

La entrega contiene:

```text
FPUNA_LogiStream_TP_Final/
│
├── README.md
├── Documentacion_LogiStream_TP_Final.pdf
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .dockerignore
├── .gitignore
│
├── pipeline/
│   └── pipeline.py
│
├── producer/
│   └── producer.py
│
└── tests/
    └── ...
```

---

## 26. Información final

**Proyecto:** LogiStream

**Asignatura:** Trabajo Práctico Final – FPUNA

**Tecnologías principales:** Python, Apache Kafka, Apache Beam y Docker.

**Objetivo:** Implementar un sistema de procesamiento de eventos logísticos en tiempo real, contemplando validación, Event Time, deduplicación, ventanas temporales, eventos fuera de orden, eventos tardíos y generación de métricas.

---

## LogiStream

Procesamiento de eventos logísticos en tiempo real.

**Apache Kafka · Apache Beam · Python · Docker**
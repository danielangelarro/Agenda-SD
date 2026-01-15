# 🏗️ Arquitectura del Sistema: RAFT + PUB/SUB

## 📊 Diagrama de Arquitectura

dame esto en formato markdown :┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  GRUPO RAFT 1   │    │  GRUPO RAFT 2   │    │  GRUPO RAFT 3   │
│  EVENTOS A-M    │◄──►│  EVENTOS N-Z    │◄──►│    GRUPOS       │
│                 │    │                 │    │                 │
│ • Líder: node1  │    │ • Líder: node2  │    │ • Líder: node3  │
│ • Seguidores    │    │ • Seguidores    │    │ • Seguidores    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └──────────────────┬──────────────────┘
                            │
                 ┌──────────────────┐
                 │  GRUPO RAFT 4    │
                 │    USUARIOS      │
                 │                  │
                 │ • Líder: node4   │
                 │ • Seguidores     │
                 └──────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│                  LAYER PUB/SUB (WebSockets)                 │
│                                                             │
│ • Notificaciones en tiempo real                            │
│ • Broadcast de eventos                                     │
│ • Manejo de conexiones clientes                            │
└─────────────────────────────────────────────────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│   Cliente 1   │       │   Cliente 2   │       │   Cliente N   │
│   (Alice)     │       │   (Bob)       │       │   (Charlie)   │
└───────────────┘       └───────────────┘       └───────────────┘

## 🎯 Opción Recomendada: RAFT + PUB/SUB

### 🗂️ RAFT para datos de agenda
- ✅ **Elección de líder** para consistencia fuerte en eventos
- ✅ **Replicación de logs** para no perder datos  
- ✅ **Failover automático** cuando un nodo falla

### 🌐 PUB/SUB para notificaciones
- ✅ **WebSockets** para notificaciones en tiempo real
- ✅ Usuarios **suscritos** a sus canales de interés
- ✅ **Desacoplado** del almacenamiento de datos

## 🛡️ Tolerancia a Fallos Completa

## 🗂️ Especificación de Grupos RAFT

### **GRUPO RAFT 1** - Eventos A-M
| Componente | Nodo | Rol | Responsabilidad |
|------------|------|-----|-----------------|
| **Líder** | node1 | 🎯 Líder | Procesar escrituras y replicar logs |
| **Réplica 1** | node4 | 📋 Seguidor | Réplica sincronizada |
| **Réplica 2** | node7 | 📋 Seguidor | Réplica sincronizada |

### **GRUPO RAFT 2** - Eventos N-Z
| Componente | Nodo | Rol | Responsabilidad |
|------------|------|-----|-----------------|
| **Líder** | node2 | 🎯 Líder | Procesar escrituras y replicar logs |
| **Réplica 1** | node5 | 📋 Seguidor | Réplica sincronizada |
| **Réplica 2** | node8 | 📋 Seguidor | Réplica sincronizada |

### **GRUPO RAFT 3** - Grupos
| Componente | Nodo | Rol | Responsabilidad |
|------------|------|-----|-----------------|
| **Líder** | node3 | 🎯 Líder | Gestión de grupos y membresías |
| **Réplica 1** | node6 | 📋 Seguidor | Réplica sincronizada |
| **Réplica 2** | node9 | 📋 Seguidor | Réplica sincronizada |

### **GRUPO RAFT 4** - Usuarios
| Componente | Nodo | Rol | Responsabilidad |
|------------|------|-----|-----------------|
| **Líder** | node10 | 🎯 Líder | Autenticación y datos de usuario |
| **Réplica 1** | node11 | 📋 Seguidor | Réplica sincronizada |
| **Réplica 2** | node12 | 📋 Seguidor | Réplica sincronizada |

## 🔄 Comunicación entre Grupos

### **Conectividad**:
- **Bidireccional** (`◄──►`) entre grupos de eventos y grupos
- **Coordinación** para operaciones transversales
- **Sincronización** de estado cuando es necesario

### **Flujo de Datos**:


## 🏗️ Arquitectura Detallada por Capas

### 1. 🖥️ **CLIENTE** (Streamlit Frontend)
```python
# Lo que tú ya tienes - Interfaz de usuario
class Cliente:
    - login_view.py
    - calendar_view.py  
    - event_view.py
    - group_view.py
    - invitations_view.py

class WebSocketManager:
    ✅ Notificaciones push inmediatas
    ✅ Sincronización en tiempo real
    ✅ Estado de conexiones activas


class SmartCoordinator:
    ✅ Decide a qué shard va cada operación
    ✅ Balanceo de carga entre shards
    ✅ Enrutamiento basado en datos
````
# 4 GRUPOS INDEPENDIENTES - Cada uno con su propio líder
Shard 1: Eventos A-M    (3 nodos: líder + 2 réplicas)
Shard 2: Eventos N-Z    (3 nodos: líder + 2 réplicas)  
Shard 3: Grupos         (3 nodos: líder + 2 réplicas)
Shard 4: Usuarios       (3 nodos: líder + 2 réplicas)

🔄 Flujo Completo de una Operación

1. 🖥️ CLIENTE (Alice)
   │
   ▼ "Crear evento: Reunión equipo - 15:00 hrs"
   │
2. 🌐 WEB SOCKET MANAGER 
   │
   ▼ Recibe la petición y la envía al coordinador
   │
3. 🎯 COORDINADOR INTELIGENTE
   │
   ▼ Analiza: "Alice" → Empieza con A → Shard Eventos A-M
   │
4. 🗂️ SHARD EVENTOS A-M (3 nodos)
   │
   ▼ Encuentra al LÍDER actual (node1)
   │
5. 🛡️ NODO LÍDER (node1)
   │
   ▼ 1. Agrega operación a su LOG
   │  2. Replica a 2 nodos seguidores (node4, node7)  
   │  3. Espera confirmación de mayoría (2/3 nodos)
   │  4. Aplica operación y confirma
   │
6. 🔄 CONFIRMACIÓN
   │
   ▼ ← ← ← ← "✅ Evento creado exitosamente"
   │
7. 📢 NOTIFICACIONES
   │
   ▼ WebSocket notifica a Bob y Charlie en tiempo real

🛡️ Protocolo de Tolerancia a Fallos
¿QUÉ PASA SI FALLA EL LÍDER?

Escenario: El líder del Shard Eventos A-M falla
1. 💥 node1 falla (se desconecta o se apaga)
   │
2. ⏰ Los seguidores (node4, node7) detectan timeout
   │   (no reciben heartbeat por 2-3 segundos)
   │
3. 🗳️ ELECCIÓN AUTOMÁTICA:
   │   - node4 se convierte en CANDIDATO  
   │   - node7 se convierte en CANDIDATO
   │   - Piden votos entre sí
   │   - node4 gana la elección (mayoría)
   │
4. 👑 NUEVO LÍDER:
   │   Shard Eventos A-M: [node1: ❌] [LÍDER: node4] [node7]
   │
5. 🔄 OPERACIONES CONTINÚAN:
   │   - Las nuevas operaciones van a node4
   │   - node4 replica a node7
   │   - Cuando node1 se recupere, se sincroniza automáticamente
   │
6. 📊 CLIENTES NO NOTAN NADA:
   │   - El coordinador redirige automáticamente al nuevo líder
   │   - Las operaciones siguen funcionando normalmente

📊 Distribución de Datos
USUARIO	SHARD	EJEMPLO
alice	Eventos A-M	Eventos de Alice van al Shard 1
bob	Eventos N-Z	Eventos de Bob van al Shard 2
charlie	Eventos A-M	Eventos de Charlie van al Shard 1
david	Eventos N-Z	Eventos de David van al Shard 2
Grupos	Shard Grupos	Todos los grupos van al Shard 3
Usuarios	Shard Usuarios	Todos los usuarios van al Shard 4

🔧 Configuración de Nodos
Topología física (ejemplo con 4 servidores):
SERVIDOR 1: [Shard1-node1, Shard2-node4, Shard3-node7, Shard4-node10]
SERVIDOR 2: [Shard1-node4, Shard2-node1, Shard3-node4, Shard4-node7]  
SERVIDOR 3: [Shard1-node7, Shard2-node7, Shard3-node1, Shard4-node4]
SERVIDOR 4: [Shard1-node10, Shard2-node10, Shard3-node10, Shard4-node1]

Resumen Visual 
CLIENTES 
    ↓
WEB SOCKETS (Tiempo real)
    ↓  
COORDINADOR (Router inteligente)
    ↓      ↓      ↓      ↓
Shard1   Shard2   Shard3   Shard4   (4 grupos independientes)
  ↓↓       ↓↓       ↓↓       ↓↓
[Líder]   [Líder]  [Líder]  [Líder]   (Cada shard tiene su propio líder)
[Réplica] [Réplica] ...     (2 réplicas por shard para tolerancia a fallos)

🧩 Combinación de Patrones y Tecnologías
1. 🗂️ SHARDING (Particionado)

    Patrón: Data Partitioning + Horizontal Scaling

    Propósito: Dividir datos por rango (A-M, N-Z) y tipo (eventos, grupos, usuarios)

2. 🛡️ RAFT CONSENSUS

    Protocolo: Consensus Algorithm + State Machine Replication

    Propósito: Tolerancia a fallos, consistencia fuerte, elección de líder

3. 🎯 SPECIALIZED LEADERSHIP

    Patrón: Command Query Responsibility Segregation (CQRS) + Service Specialization

    Propósito: Líderes especializados por tipo de dato para evitar cuello de botella

4. 🌐 PUB/SUB + WEB SOCKETS

    Patrón: Publish-Subscribe + Real-Time Communication

    Propósito: Notificaciones inmediatas, sincronización en tiempo real

5. 🎯 INTELLIGENT ROUTING

    Patrón: Router + Load Balancer

    Propósito: Enrutamiento inteligente basado en datos y tipo de operación

SHARDING (Escalabilidad) + RAFT (Consistencia) 
    ↓
MULTI-LEADER (Rendimiento) + SPECIALIZATION (Eficiencia)
    ↓  
REAL-TIME LAYER (Experiencia) + INTELLIGENT ROUTING (Balanceo)

✅ Beneficios Clave

    🔄 Alta Disponibilidad: Tolerancia a fallos de nodos

    ⚡ Alto Rendimiento: Múltiples líderes especializados

    📈 Escalabilidad Horizontal: Sharding por tipo de dato

    🔒 Consistencia Fuerte: Protocolo RAFT garantizado

    🌐 Tiempo Real: Notificaciones inmediatas vía WebSockets

    🎯 Balanceo Inteligente: Enrutamiento basado en datos
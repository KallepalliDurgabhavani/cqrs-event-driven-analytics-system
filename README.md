# CQRS and Event-Driven Analytics System

A high-performance e-commerce analytics system built using the **CQRS** (Command Query Responsibility Segregation) pattern and **Event-Driven Architecture**.

## 🚀 Architecture Overview

This system separates write and read operations to ensure scalability and high performance:

- **Command Service**: Handles write operations (Product creation, Order processing) and publishes events via the **Transactional Outbox Pattern**.
- **Message Broker (RabbitMQ)**: Facilitates asynchronous communication between services.
- **Consumer Service**: Listens for events and updates materialized views in the read database.
- **Query Service**: Provides highly optimized read-only endpoints for dashboard analytics.

## 🛠️ Tech Stack

- **Runtime**: Node.js (v20)
- **ORM**: Prisma (v6)
- **Databases**: PostgreSQL (Write DB & Read DB)
- **Messaging**: RabbitMQ
- **Containerization**: Docker & Docker Compose

## 🚦 Getting Started

### Prerequisites
- Docker & Docker Compose installed.

### Setup & Run
1. Clone the repository.
2. Build and start all services:
   ```bash
   docker-compose up --build
   ```
3. The databases will automatically initialize their schemas on startup.

## 📡 API Endpoints

### Command Service (Port 8080)
- `POST /api/products`: Create a new product.
- `POST /api/orders`: Place a new order (triggers events).

### Query Service (Port 8081)
- `GET /api/analytics/products/:productId/sales`: Total sales for a product.
- `GET /api/analytics/categories/:category/revenue`: Revenue by category.
- `GET /api/analytics/customers/:customerId/lifetime-value`: Customer LTV.
- `GET /api/analytics/sync-status`: Lag between write and read models.

## 🛠️ Verification
Detailed verification steps are available in `verify.md`. You can use the provided `curl` commands to test the end-to-end flow from product creation to analytics retrieval.

## 📂 Project Structure
- `/command-service`: Write model logic & outbox poller.
- `/consumer-service`: Event listener & read model updater.
- `/query-service`: Read model API.
- `docker-compose.yml`: Infrastructure orchestration."# cqrs-event-driven-analytics-system" 

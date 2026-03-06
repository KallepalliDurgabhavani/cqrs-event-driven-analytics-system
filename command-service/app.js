const express = require('express');
const { PrismaClient } = require('@prisma/client');
const amqp = require('amqplib');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
app.use(express.json());

const prisma = new PrismaClient();
const PORT = process.env.COMMAND_SERVICE_PORT || 8080;

// Health check
app.get('/health', (req, res) => res.status(200).send('OK'));

// POST /api/products
app.post('/api/products', async (req, res) => {
    try {
        const { name, category, price, stock } = req.body;
        const product = await prisma.product.create({
            data: { name, category, price, stock }
        });
        res.status(201).json({ productId: product.id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// POST /api/orders
app.post('/api/orders', async (req, res) => {
    const { customerId, items } = req.body;

    try {
        const result = await prisma.$transaction(async (tx) => {
            let total = 0;
            const orderItemsData = [];

            for (const item of items) {
                const product = await tx.product.findUnique({ where: { id: item.productId } });
                if (!product || product.stock < item.quantity) {
                    throw new Error(`Insufficient stock for product ${item.productId}`);
                }

                await tx.product.update({
                    where: { id: item.productId },
                    data: { stock: { decrement: item.quantity } }
                });

                total += item.price * item.quantity;
                orderItemsData.push({
                    productId: item.productId,
                    quantity: item.quantity,
                    price: item.price,
                    category: product.category // Added category
                });
            }

            const order = await tx.order.create({
                data: {
                    customerId,
                    total,
                    status: 'PENDING',
                    items: {
                        create: orderItemsData.map(({ productId, quantity, price }) => ({ productId, quantity, price }))
                    }
                },
                include: { items: true }
            });

            // Transactional Outbox
            await tx.outbox.create({
                data: {
                    topic: 'order-events',
                    payload: {
                        eventType: 'OrderCreated',
                        orderId: order.id,
                        customerId: order.customerId,
                        items: orderItemsData, // Includes category
                        total: order.total,
                        timestamp: order.createdAt
                    }
                }
            });

            return order;
        });

        res.status(201).json({ orderId: result.id });
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

// Outbox Poller
const pollOutbox = async () => {
    let connection;
    try {
        connection = await amqp.connect(process.env.BROKER_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue('order-events', { durable: true });

        console.log('Outbox poller started...');

        setInterval(async () => {
            try {
                const messages = await prisma.outbox.findMany({
                    where: { publishedAt: null },
                    take: 10
                });

                for (const msg of messages) {
                    channel.sendToQueue(msg.topic, Buffer.from(JSON.stringify(msg.payload)), { persistent: true });
                    await prisma.outbox.update({
                        where: { id: msg.id },
                        data: { publishedAt: new Date() }
                    });
                    console.log(`Published event ${msg.id} to topic ${msg.topic}`);
                }
            } catch (err) {
                console.error('Error polling outbox:', err.message);
            }
        }, 5000);

    } catch (error) {
        console.error('Failed to connect to RabbitMQ for polling:', error.message);
        setTimeout(pollOutbox, 10000);
    }
};

app.listen(PORT, () => {
    console.log(`Command Service running on port ${PORT}`);
    pollOutbox();
});

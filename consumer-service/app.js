const amqp = require('amqplib');
const { PrismaClient } = require('@prisma/client');
const dotenv = require('dotenv');

dotenv.config();

const prisma = new PrismaClient();

const processOrderCreated = async (event) => {
    const { eventType, orderId, customerId, items, total, timestamp } = event;

    try {
        await prisma.$transaction(async (tx) => {
            // Idempotency check
            const existingEvent = await tx.processedEvent.findUnique({
                where: { eventId: String(orderId) } // Using orderId as eventId for OrderCreated
            });

            if (existingEvent) {
                console.log(`Event ${orderId} already processed. Skipping.`);
                return;
            }

            // Update Product Sales View
            for (const item of items) {
                await tx.productSalesView.upsert({
                    where: { productId: item.productId },
                    update: {
                        totalQuantitySold: { increment: item.quantity },
                        totalRevenue: { increment: item.price * item.quantity },
                        orderCount: { increment: 1 }
                    },
                    create: {
                        productId: item.productId,
                        totalQuantitySold: item.quantity,
                        totalRevenue: item.price * item.quantity,
                        orderCount: 1
                    }
                });
            }

            // Update Category Metrics View (Assume category is provided in items or fetched)
            // For simplicity, we'll need category. In a real app, it's better to include it in the event.
            // Since it's not in the event payload yet, let's assume we fetch it if missing or update app.js to include it.
            // I'll update app.js later to include category in items.
            for (const item of items) {
                // We'll skip category update here and fix app.js to include it in the event for efficiency
                // or we fetch it from write-side (which breaks CQRS slightly if they are separate DBs)
                // Let's assume the event has category for now (I will fix app.js)
                if (item.category) {
                    await tx.categoryMetricsView.upsert({
                        where: { categoryName: item.category },
                        update: {
                            totalRevenue: { increment: item.price * item.quantity },
                            totalOrders: { increment: 1 }
                        },
                        create: {
                            categoryName: item.category,
                            totalRevenue: item.price * item.quantity,
                            totalOrders: 1
                        }
                    });
                }
            }

            // Update Customer LTV View
            await tx.customerLtvView.upsert({
                where: { customerId: customerId },
                update: {
                    totalSpent: { increment: total },
                    orderCount: { increment: 1 },
                    lastOrderDate: new Date(timestamp)
                },
                create: {
                    customerId: customerId,
                    totalSpent: total,
                    orderCount: 1,
                    lastOrderDate: new Date(timestamp)
                }
            });

            // Update Hourly Sales View
            const date = new Date(timestamp);
            date.setMinutes(0, 0, 0); // Round down to the hour
            const hourTimestamp = date;

            await tx.hourlySalesView.upsert({
                where: { hourTimestamp: hourTimestamp },
                update: {
                    totalOrders: { increment: 1 },
                    totalRevenue: { increment: total }
                },
                create: {
                    hourTimestamp: hourTimestamp,
                    totalOrders: 1,
                    totalRevenue: total
                }
            });

            // Mark event as processed
            await tx.processedEvent.create({
                data: {
                    eventId: String(orderId),
                    processedAt: new Date()
                }
            });

            console.log(`Processed OrderCreated for order ${orderId}`);
        });
    } catch (error) {
        console.error(`Error processing event for order ${orderId}:`, error.message);
        throw error; // Re-throw to trigger RabbitMQ retry/DLQ
    }
};

const startConsumer = async () => {
    try {
        const connection = await amqp.connect(process.env.BROKER_URL);
        const channel = await connection.createChannel();
        const queue = 'order-events';

        await channel.assertQueue(queue, { durable: true });
        channel.prefetch(1);

        console.log('Consumer waiting for messages...');

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                const content = JSON.parse(msg.content.toString());
                console.log(`Received event: ${content.eventType}`);

                try {
                    if (content.eventType === 'OrderCreated') {
                        await processOrderCreated(content);
                    }
                    channel.ack(msg);
                } catch (err) {
                    console.error('Failed to process message, nacking...');
                    channel.nack(msg);
                }
            }
        });

    } catch (error) {
        console.error('Consumer error:', error.message);
        setTimeout(startConsumer, 10000);
    }
};

startConsumer();

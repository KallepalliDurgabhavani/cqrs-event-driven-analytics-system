const express = require('express');
const { PrismaClient } = require('@prisma/client');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const prisma = new PrismaClient();
const PORT = process.env.QUERY_SERVICE_PORT || 8081;

// Health check
app.get('/health', (req, res) => res.status(200).send('OK'));

// GET /api/analytics/products/{productId}/sales
app.get('/api/analytics/products/:productId/sales', async (req, res) => {
    try {
        const productId = parseInt(req.params.productId);
        const sales = await prisma.productSalesView.findUnique({
            where: { productId }
        });

        if (!sales) {
            return res.status(200).json({
                productId,
                totalQuantitySold: 0,
                totalRevenue: 0,
                orderCount: 0
            });
        }

        res.status(200).json(sales);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// GET /api/analytics/categories/{category}/revenue
app.get('/api/analytics/categories/:category/revenue', async (req, res) => {
    try {
        const categoryName = req.params.category;
        const metrics = await prisma.categoryMetricsView.findUnique({
            where: { categoryName }
        });

        if (!metrics) {
            return res.status(200).json({
                category: categoryName,
                totalRevenue: 0,
                totalOrders: 0
            });
        }

        res.status(200).json({
            category: metrics.categoryName,
            totalRevenue: metrics.totalRevenue,
            totalOrders: metrics.totalOrders
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// GET /api/analytics/customers/{customerId}/lifetime-value
app.get('/api/analytics/customers/:customerId/lifetime-value', async (req, res) => {
    try {
        const customerId = parseInt(req.params.customerId);
        const ltv = await prisma.customerLtvView.findUnique({
            where: { customerId }
        });

        if (!ltv) {
            return res.status(200).json({
                customerId,
                totalSpent: 0,
                orderCount: 0,
                lastOrderDate: null
            });
        }

        res.status(200).json(ltv);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// GET /api/analytics/sync-status
app.get('/api/analytics/sync-status', async (req, res) => {
    try {
        const lastEvent = await prisma.processedEvent.findFirst({
            orderBy: { processedAt: 'desc' }
        });

        if (!lastEvent) {
            return res.status(200).json({
                lastProcessedEventTimestamp: null,
                lagSeconds: 0
            });
        }

        const lagSeconds = (new Date().getTime() - lastEvent.processedAt.getTime()) / 1000;

        res.status(200).json({
            lastProcessedEventTimestamp: lastEvent.processedAt.toISOString(),
            lagSeconds: Math.max(0, lagSeconds)
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Query Service running on port ${PORT}`);
});

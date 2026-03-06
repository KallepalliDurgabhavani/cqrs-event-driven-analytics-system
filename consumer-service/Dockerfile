FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY prisma ./prisma
RUN npx prisma generate

COPY . .

CMD ["sh", "-c", "npx prisma db push && node app.js"]

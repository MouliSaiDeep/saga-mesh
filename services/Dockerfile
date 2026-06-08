# Multi-stage build for microservices
FROM node:20-alpine AS builder

WORKDIR /usr/src/app

COPY package*.json ./
COPY tsconfig.json ./

# Install dev dependencies
RUN npm ci

# Copy shared logic & local service code
COPY shared-utils.ts ./services/shared-utils.ts
COPY order-service/index.ts ./services/order-service/index.ts
COPY outbox-relay/index.ts ./services/outbox-relay/index.ts
COPY inventory-service/index.ts ./services/inventory-service/index.ts
COPY payment-service/index.ts ./services/payment-service/index.ts
COPY notification-service/index.ts ./services/notification-service/index.ts
COPY dashboard/index.ts ./services/dashboard/index.ts

# Build typescript project
RUN npm run build

# Production container running as non-root user
FROM node:20-alpine AS runner

WORKDIR /usr/src/app

COPY package*.json ./

# Install production dependencies only
RUN npm ci --only=production

# Copy compiled files
COPY --from=builder /usr/src/app/dist ./dist

# Create user node if not exists (exists by default in alpine node image), assign directory owner
RUN chown -R node:node /usr/src/app

USER node

# Default command - will override via docker-compose
CMD ["node", "dist/order-service/index.js"]

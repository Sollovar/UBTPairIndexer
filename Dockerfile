# Build stage
FROM node:20-alpine as builder

WORKDIR /build

# Copy package files
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci --omit=dev

# Runtime stage
FROM node:20-alpine

WORKDIR /app

# Copy from builder
COPY --from=builder /build/node_modules ./node_modules

# Copy application files
COPY index.js package.json ./

EXPOSE 3001

CMD ["node", "index.js"]

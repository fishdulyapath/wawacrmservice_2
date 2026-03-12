FROM node:20-alpine

WORKDIR /app
ENV NODE_ENV=production

# Install dependencies first (cache layer)
COPY package*.json ./
RUN npm ci --omit=dev

# Copy source
COPY . .

# Create uploads directory
RUN mkdir -p uploads/activities

EXPOSE 3000

CMD ["node", "index.js"]

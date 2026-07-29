FROM node:22-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

ENV ASTRO_TELEMETRY_DISABLED=1
EXPOSE 4312

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "4312"]

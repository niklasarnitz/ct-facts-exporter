# syntax=docker/dockerfile:1
# Production image (replaces Coolify's nixpacks build): Bun 1.3 runs src/index.ts directly on port 3000.
# Built by .github/workflows/image.yml and deployed by Komodo from niklasarnitz/ops (stacks/ct-facts-exporter).
# src/db.ts opens data.db in the working directory, so the working directory is /data (a volume); the app
# itself lives in /app. CT_BASE_URL and CT_LOGIN_TOKEN come from the stack's environment at runtime.
FROM oven/bun:1.3.0
WORKDIR /app
COPY package.json bun.lock ./
RUN --mount=type=cache,target=/root/.bun/install/cache bun install --frozen-lockfile --production
COPY tsconfig.json ./
COPY src ./src
WORKDIR /data
ENV NODE_ENV=production PORT=3000
EXPOSE 3000
CMD ["bun", "/app/src/index.ts"]

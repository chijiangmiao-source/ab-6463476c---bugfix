# Verify runs once and exits; its exit code is the check result.
FROM node:20-alpine
WORKDIR /app
COPY core/ core/
COPY frontend/package.json frontend/package-lock.json frontend/
RUN cd frontend && npm ci
COPY frontend/ frontend/
COPY verify/ verify/
ENV WEB_URL=http://web:8080
CMD ["node", "verify/run.js"]

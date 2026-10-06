# Multi-stage: build the frontend bundle, then serve it with the static server.
FROM node:20-alpine AS frontend-build
WORKDIR /app
COPY core/ core/
COPY frontend/package.json frontend/package-lock.json frontend/
RUN cd frontend && npm ci
COPY frontend/ frontend/
RUN cd frontend && node build.js

FROM node:20-alpine
ENV PORT=8080
WORKDIR /srv
COPY web/package.json web/server.js ./
COPY --from=frontend-build /app/frontend/dist ./public
EXPOSE 8080
CMD ["node", "server.js"]

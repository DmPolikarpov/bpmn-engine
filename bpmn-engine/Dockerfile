# 1. Этап сборки (Builder stage)
FROM rust:1.89-alpine AS builder

# Устанавливаем системные зависимости для компиляции C-кода 🛠️
RUN apk add --no-cache musl-dev build-base

WORKDIR /usr/src/app

# Шаг A: Копируем ТОЛЬКО файлы зависимостей
COPY Cargo.toml Cargo.lock* ./

# Шаг B: Создаем временный пустой проект и собираем зависимости
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Шаг C: Копируем реальный исходный код и собираем наше приложение
COPY migrations ./migrations
COPY src ./src
RUN touch src/main.rs && cargo build --release

# 2. Финальный этап (Final stage)
FROM alpine:3.19

WORKDIR /app
COPY --from=builder /usr/src/app/target/release/bpmn-engine /app/bpmn-engine

EXPOSE 8080
CMD ["./bpmn-engine"]
# =========================================================
# Этап 1: Компиляция приложения (Builder)
# =========================================================
FROM rust:1.75-slim as builder

WORKDIR /usr/src/app

# Копируем файлы зависимостей для кэширования слоев Docker
COPY Cargo.toml Cargo.lock ./

# Создаем фиктивный main.rs, чтобы скомпилировать только зависимости
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Копируем реальный исходный код и собираем финальный бинарный файл
COPY src ./src
# Обновляем timestamp исходников, чтобы cargo пересобрал наш код, а не использовал фиктивный
RUN touch src/main.rs
RUN cargo build --release --bin bpmn-engine-inst

# =========================================================
# Этап 2: Минимальный образ для запуска (Runtime)
# =========================================================
FROM debian:bookworm-slim

# Устанавливаем OpenSSL и сертификаты для работы с сетевыми подключениями (TLS/SSL)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем скомпилированный бинарник из этапа builder
COPY --from=builder /usr/src/app/target/release/bpmn-engine-inst /app/bpmn-engine-inst

# Переменные окружения по умолчанию
ENV RUST_LOG=info

# Открываем порт, если инстанс принимает подключения напрямую (например, healthcheck)
EXPOSE 8080

# Запуск исполнителя
ENTRYPOINT ["/app/bpmn-engine-inst"]
# Используем легковесный образ Node.js на базе Alpine Linux
FROM node:20-alpine

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /usr/src/app

# Копируем файлы манифестов зависимостей
COPY package*.json ./

# Устанавливаем только production-зависимости
RUN npm install --omit=dev

# Копируем исходный код микросервиса
COPY index.js ./

# Переключаемся на непривилегированного пользователя node
USER node

# Открываем порт 8080 для установки WebSocket-соединений
EXPOSE 8080

# Команда запуска WebSocket Cluster
CMD ["node", "index.js"]
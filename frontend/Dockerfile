FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

ARG VITE_SIGNALING_URL=ws://localhost:8080
ENV VITE_SIGNALING_URL=$VITE_SIGNALING_URL

EXPOSE 3000

CMD ["npm", "run", "dev"]

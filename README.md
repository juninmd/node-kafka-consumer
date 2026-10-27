# node-kafka-consumer

Consumidor Kafka em Node.js/TypeScript (kafkajs) com um servidor Express mínimo.

## Instalação

```bash
yarn install
```

## Uso

1. Suba o Kafka local (Zookeeper + broker, tópico `topic-test` criado automaticamente):
   ```bash
   export HOST_IP=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1)
   docker-compose up
   ```
2. Ajuste os brokers em `src/configs/index.ts`.
3. Variáveis de ambiente podem ser carregadas de `envs/.env-<NODE_ENV>` (veja `src/tools/envs.tools.ts`); `PORT` (padrão 9000) define a porta HTTP e `KAFKA_TOPIC` (padrão `topic-test`) o tópico consumido.
4. O consumidor está em `src/consumer/index.ts`: chame `ConsumerKafka.getInstance().start()` para conectar, assinar o tópico e iniciar o consumo. A lógica por mensagem fica em `eachMessage`.

## Estrutura

- `src/index.ts`: ponto de entrada (carrega envs e sobe o servidor).
- `src/server.ts` e `src/api/`: API HTTP (Express).
- `src/consumer/`: consumidor Kafka.
- `src/configs/`: configuração do Kafka.
- `docker-compose.yml`: Kafka local para desenvolvimento.

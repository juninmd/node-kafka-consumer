import { Kafka, Consumer } from 'kafkajs';
import configs from '../configs';

/**
 * Classe Singleton
 */
export default class ConsumerKafka {
  private static instance: ConsumerKafka;
  public static getInstance(): ConsumerKafka {
    if (!ConsumerKafka.instance) {
      ConsumerKafka.instance = new ConsumerKafka();
    }
    return ConsumerKafka.instance;
  }

  private consumer: Consumer;

  /**
   * Colocamos como private para impedir a instância via new
   */
  private constructor() {
    const kafka = new Kafka(configs.kafka)
    this.consumer = kafka.consumer({ groupId: 'test-group' })
  }

  /**
   * Conecta, assina o tópico e só então inicia o consumo.
   * O kafkajs exige a ordem connect -> subscribe -> run.
   */
  async start(topic: string = process.env.KAFKA_TOPIC || 'topic-test') {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic, fromBeginning: false });
    await this.consumer.run({
      eachMessage: this.eachMessage,
    });
  }

  async eachMessage({ topic, partition, message }) {
    console.log({
      topic,
      partition,
      // message.value é null em tombstones (mensagens sem corpo)
      value: message.value?.toString() ?? null,
    })
  }

  async disconnect() {
    return await this.consumer.disconnect();
  }
}

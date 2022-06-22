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
    this.consumer.run({
      eachMessage: this.eachMessage,
    })
    this.consumer.connect();
  }

  async eachMessage({ topic, partition, message }) {
    console.log({
      topic,
      partition,
      value: message.value.toString(),
    })
  }

  async disconnect() {
    return await this.consumer.disconnect();
  }
}

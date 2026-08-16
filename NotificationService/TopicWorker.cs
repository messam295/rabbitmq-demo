using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NotificationService;

public class TopicWorker(RabbitMqConnection connection,
    ILogger<TopicWorker> logger,
    string consumerName,
    string queueName,
    string bindingPattern)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection!.Connection.CreateChannelAsync(cancellationToken: stoppingToken);
        var topic = "orders.topic";
        
        await channel.ExchangeDeclareAsync(exchange: topic,
            type: ExchangeType.Topic,
            durable: true,
            cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        await channel.QueueBindAsync(queue: queueName,
            exchange: topic,
            routingKey: bindingPattern,
            cancellationToken: stoppingToken);
        
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var json = Encoding.UTF8.GetString(body);
            var order = JsonSerializer.Deserialize<OrderMessage>(json);

            // ea.Redelivered = true means RabbitMQ already tried delivering this message before.
            // It does NOT mean it was successfully processed — the previous consumer may have
            // crashed right after receiving it. Always process idempotently (safe to run twice).
            logger.LogInformation(
                "[{consumerName}] -[Notification] New order received — Product: {Product}, Quantity: {Qty} (redelivered: {Redelivered})",
                consumerName, order?.ProductName, order?.Quantity, eventArgs.Redelivered);

            // Manual ACK: tells RabbitMQ "I'm done, remove this message from the queue."
            // multiple: false → ACK only this specific message, not all previous ones.
            // Because autoAck is false below, skipping this would leave the message
            // as unACKed until consumer_timeout fires or this service disconnects.
            await channel.BasicAckAsync(eventArgs.DeliveryTag,
                multiple: false,
                cancellationToken: stoppingToken);
        };

        // autoAck: false → we control when messages are acknowledged (safer).
        // autoAck: true would ACK immediately on delivery, before we process —
        // meaning a crash mid-processing would silently lose the message.
        await channel.BasicConsumeAsync(queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        // Keep the worker alive. The consumer runs on RabbitMQ's event loop in the background.
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}


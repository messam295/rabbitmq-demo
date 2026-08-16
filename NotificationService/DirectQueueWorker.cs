using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NotificationService;

public class DirectQueueWorker(RabbitMqConnection connection,
    ILogger<DirectQueueWorker> logger,
    string consumerName)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection!.Connection.CreateChannelAsync(cancellationToken: stoppingToken);
        // Declare the same queue as OrderService. Safe to declare from multiple services —
        // RabbitMQ checks that settings match and ignores it if the queue already exists.
        await channel.QueueDeclareAsync(
            queue: "orders",
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        // prefetchCount: 1 → RabbitMQ won't send us a new message until we ACK the current one.
        // This prevents this consumer from hoarding messages it hasn't processed yet,
        // and ensures fair distribution when multiple NotificationService instances run.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var body = ea.Body.ToArray();
            var json = Encoding.UTF8.GetString(body);
            var order = JsonSerializer.Deserialize<OrderMessage>(json);

            // ea.Redelivered = true means RabbitMQ already tried delivering this message before.
            // It does NOT mean it was successfully processed — the previous consumer may have
            // crashed right after receiving it. Always process idempotently (safe to run twice).
            logger.LogInformation(
                "[{consumerName}] -[Notification] New order received — Product: {Product}, Quantity: {Qty} (redelivered: {Redelivered})",
                consumerName, order?.ProductName, order?.Quantity, ea.Redelivered);

            // Manual ACK: tells RabbitMQ "I'm done, remove this message from the queue."
            // multiple: false → ACK only this specific message, not all previous ones.
            // Because autoAck is false below, skipping this would leave the message
            // as unACKed until consumer_timeout fires or this service disconnects.
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };

        // autoAck: false → we control when messages are acknowledged (safer).
        // autoAck: true would ACK immediately on delivery, before we process —
        // meaning a crash mid-processing would silently lose the message.
        await channel.BasicConsumeAsync(queue: "orders", autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

        // Keep the worker alive. The consumer runs on RabbitMQ's event loop in the background.
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
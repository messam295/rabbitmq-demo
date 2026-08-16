using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapPost("/orders", async (OrderRequest request) =>
{
    // ConnectionFactory holds the connection config (host, port, credentials)
    var factory = new ConnectionFactory { HostName = "localhost" };

    // One TCP connection to RabbitMQ — expensive to create, so in production
    // you'd keep this alive (singleton). Here we create per-request for simplicity.
    await using var connection = await factory.CreateConnectionAsync();

    // A channel is a lightweight virtual connection inside the TCP connection.
    // All RabbitMQ operations (publish, consume, declare) happen on a channel.
    await using var channel = await connection.CreateChannelAsync();

    const string topic = "orders.topic";
    
    // Declare the queue. Safe to call every time — RabbitMQ ignores it if the queue already exists.
    // durable: true  → queue survives a RabbitMQ restart
    // exclusive: false → other connections can access this queue
    // autoDelete: false → queue stays even when no consumers are connected
    await channel.QueueDeclareAsync(
        queue: "orders",
        durable: true,
        exclusive: false,
        autoDelete: false);

    await channel.ExchangeDeclareAsync(exchange: topic,
        type: ExchangeType.Topic,
        durable: true);

    var message = JsonSerializer.Serialize(request);
    var body = Encoding.UTF8.GetBytes(message);

    // Persistent = true → message is written to disk, survives a RabbitMQ restart.
    // Without this, messages live only in memory and are lost on crash/restart.
    var props = new BasicProperties { Persistent = true };

    // Publish to the default exchange (exchange: "").
    // The default exchange is a built-in direct exchange — it routes to the queue
    // whose name matches the routingKey exactly. No manual binding needed.
    // await channel.BasicPublishAsync(
    //     exchange: "",
    //     routingKey: "orders",   // delivers to the queue named "orders"
    //     mandatory: false,
    //     basicProperties: props,
    //     body: body);

    await channel.BasicPublishAsync(
        exchange: topic,
        routingKey: $"orders.{request.Region}.{request.Priority}",
        mandatory: false,
        basicProperties: props,
        body: body);
    
    return Results.Ok(new { Status = "Order placed", request.ProductName });
});

app.Run();

record OrderRequest(string ProductName, int Quantity, string Region, string Priority);

using RabbitMQ.Client;

namespace NotificationService;

public class RabbitMqConnection: IAsyncDisposable
{
    private readonly IConnection _connection;
    public IConnection Connection => _connection;

    public static async Task<RabbitMqConnection> CreateAsync()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var connection = await factory.CreateConnectionAsync();
        return new RabbitMqConnection(connection);
    }

    private RabbitMqConnection(IConnection connection) => _connection = connection;

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();
}
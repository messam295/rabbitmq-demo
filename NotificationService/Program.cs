using NotificationService;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<RabbitMqConnection>(_ =>
    RabbitMqConnection.CreateAsync().GetAwaiter().GetResult());

builder.Services.AddSingleton<IHostedService>(sp =>
    new DirectQueueWorker(sp.GetRequiredService<RabbitMqConnection>(),
        sp.GetRequiredService<ILogger<DirectQueueWorker>>(), "Consumer-1"));

builder.Services.AddSingleton<IHostedService>(sp =>
    new DirectQueueWorker(sp.GetRequiredService<RabbitMqConnection>(),
        sp.GetRequiredService<ILogger<DirectQueueWorker>>(), "Consumer-2"));

builder.Services.AddSingleton<IHostedService>(sp =>
    new TopicWorker(sp.GetRequiredService<RabbitMqConnection>(),
        sp.GetRequiredService<ILogger<TopicWorker>>(),
        "Consumer-3",
        "orders.high-priority",
        "orders.*.high"));

builder.Services.AddSingleton<IHostedService>(sp =>
    new TopicWorker(sp.GetRequiredService<RabbitMqConnection>(),
        sp.GetRequiredService<ILogger<TopicWorker>>(),
        "Consumer-4",
        "orders.US",
        "orders.US.#"));

builder.Services.AddSingleton<IHostedService>(sp =>
    new TopicWorker(sp.GetRequiredService<RabbitMqConnection>(),
        sp.GetRequiredService<ILogger<TopicWorker>>(),
        "Consumer-5",
        "orders.all",
        "orders.#"));

var host = builder.Build();
host.Run();

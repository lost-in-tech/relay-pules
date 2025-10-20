using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace RelayPulse.RabbitMQ;

internal interface IRabbitMqConnectionInstance
{
    IConnection Get();
}

internal interface IRabbitMqConnectionSettings
{
    public string Uri { get; }
}

internal sealed class RabbitMqConnectionInstance : IRabbitMqConnectionInstance
{
    private readonly Lazy<IConnection> _instance;
    
    public RabbitMqConnectionInstance(IRabbitMqConnectionSettings settings,
        ILogger<RabbitMqConnectionInstance> logger)
    {
        _instance = new Lazy<IConnection>(() =>
        {
            if (string.IsNullOrWhiteSpace(settings.Uri))
                throw new Exception("No connection string provided for rabbitmq");
            
            var factory = new ConnectionFactory
            {
                DispatchConsumersAsync = true,
                Uri = new Uri(settings.Uri)
            };

            var connection = factory.CreateConnection();

            logger.LogInformation("RabbitMQ connection established.", settings.Uri);

            connection.ConnectionShutdown += (_, args) =>
            {
                logger.LogWarning("RabbitMQ connection shutdown. Reason: {reason}", args.Cause);
            };

            connection.ConnectionBlocked += (_, args) =>
            {
                logger.LogWarning("RabbitMQ connection blocked. Reason: {reason}", args.Reason);
            };

            connection.ConnectionUnblocked += (_, args) =>
            {
                logger.LogInformation("RabbitMQ connection unblocked.");
            };

            return connection;
        });
    }

    public IConnection Get() => _instance.Value;
}
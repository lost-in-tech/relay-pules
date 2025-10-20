using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RelayPulse.Core;

namespace RelayPulse.RabbitMQ.Subscribers;

internal sealed class MessageListener(
    SetupRabbitMq setupRabbitMq,
    QueueSettingsValidator validator,
    IQueueSettings settings,
    IChannelFactory channelFactory,
    MessageSubscriber subscriber,
    ILogger<MessageListener> logger) : IMessageListener, ISetupRabbitMq, IDisposable
{
    private QueueInfo[] _queues = Array.Empty<QueueInfo>();
    private readonly List<IModel> _channels = new();

    private Task Init()
    {
        validator.Validate(settings);
        
        _queues = setupRabbitMq.Run(settings);

        return Task.CompletedTask;
    }

    public async Task Listen(CancellationToken ct)
    {
        await Init();
        
        foreach (var queue in _queues)
        {
            var channel = channelFactory.GetOrCreate(queue.Name);

            channel.QueueDeclarePassive(queue.Name);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (_, args) =>
            {
                // Use the consumer's current model to avoid using a stale/closed channel after recovery
                await subscriber.Subscribe(consumer.Model, queue, args, ct);
            };
            consumer.ConsumerCancelled += (_, args) =>
            {
                logger.LogWarning("RabbitMQ consumer cancelled, {tags}", args.ConsumerTags?.Join(","));
                return Task.CompletedTask;
            };

            consumer.Unregistered += (_, args) =>
            {
                logger.LogWarning("RabbitMQ consumer unregistered, {tags}", args.ConsumerTags?.Join(","));
                return Task.CompletedTask;
            };

            consumer.Registered += (_, args) =>
            {
                logger.LogInformation("RabbitMQ consumer registered. {tags}", args.ConsumerTags?.Join(","));
                return Task.CompletedTask;
            };

            consumer.Shutdown += (_, args) =>
            {
                logger.LogError("RabbitMQ consumer shutdown. {tags}", args.Cause);
                return Task.CompletedTask;
            };
            
            if (queue.PrefetchCount is > 0)
            {
                channel.BasicQos(0, (ushort)queue.PrefetchCount.Value, false);
            }

            channel.BasicConsume(queue.Name, false, Guid.NewGuid().ToString(), false, false, null, consumer);

            channel.ModelShutdown += (_, args) =>
            {
                logger.LogError("RabbitMQ channel shutdown, {cause}", args.Cause);
            };
            
            _channels.Add(channel);
        }
    }

    public async Task ListenUntilCancelled(CancellationToken ct)
    {
        logger.LogInformation("Start listening.");
        
        await Listen(ct);

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(10000, ct);
            
            logger.LogTrace("Listener listening...");
        }
    }

    public void Dispose()
    {
        foreach (var channel in _channels)
        {
            if (!channel.IsClosed)
            {
                channel.Close();
            }
            
            channel.Dispose();
        }
    }

    Task ISetupRabbitMq.Run(CancellationToken ct)
    {
        return Init();
    }
}
using RabbitMQ.Client;

namespace RelayPulse.RabbitMQ.Subscribers;

internal class SetupRabbitMq(
    IRabbitMqWrapper wrapper,
    IRabbitMqConnectionInstance connectionInstance)
{
    public QueueInfo[] Run(IQueueSettings settings)
    {
        var result = new List<QueueInfo>();

        if (settings.Queues == null) return Array.Empty<QueueInfo>();

        var exchangeCreated = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using var channel = connectionInstance.Get().CreateModel();

        foreach (var queue in settings.Queues)
        {
            var exchange = queue.Exchange.TryPickNonEmpty(settings.DefaultExchange) ?? string.Empty;

            if (!exchange.HasValue()) throw new Exception("Exchange name cannot be empty");

            var exchangeType = queue.ExchangeType.TryPickNonEmpty(settings.DefaultExchangeType)
                .EmptyAlternative(ExchangeTypesSupported.Direct);

            var deadLetterExchange = queue.DeadLetterDisabled
                ? null
                : queue.DeadLetterExchange.EmptyAlternative(
                    settings.DefaultDeadLetterExchange.EmptyAlternative(DefaultDeadLetterExchange(exchange)));
            var deadLetterQueue = queue.DeadLetterDisabled
                ? null
                : queue.DeadLetterQueue.EmptyAlternative(DefaultDeadLetterQueue(queue.Name));


            var retryExchange = queue.RetryDisabled
                ? null
                : queue.RetryExchange.EmptyAlternative(
                    settings.DefaultRetryExchange.EmptyAlternative(DefaultRetryExchange(exchange)));

            if (!exchangeCreated.ContainsKey(exchange))
            {
                if (!(queue.SkipSetup ?? false))
                {
                    wrapper.ExchangeDeclare(channel, exchange, exchangeType);
                }

                exchangeCreated[exchange] = exchange;
            }

            var queueArgs = new Dictionary<string, object>();

            if (deadLetterExchange.HasValue())
            {
                queueArgs[Constants.HeaderDeadLetterExchange] = deadLetterExchange;
                queueArgs[Constants.HeaderDeadLetterRoutingKey] =
                    queue.DeadLetterRoutingKey.EmptyAlternative(queue.Name);
            }

            if (queue.MsgExpiryInSeconds is > 0)
            {
                queueArgs[Constants.HeaderTimeToLive] = queue.MsgExpiryInSeconds.Value * 1000;
            }

            wrapper.QueueDeclare(channel, queue.Name, queueArgs);
            

            if (deadLetterExchange.HasValue()
                && deadLetterQueue.HasValue())
            {
                if (!exchangeCreated.ContainsKey(deadLetterExchange))
                {
                    wrapper.ExchangeDeclare(channel,
                        deadLetterExchange,
                        queue.DeadLetterExchangeType.EmptyAlternative(
                            settings.DefaultDeadLetterExchangeType.EmptyAlternative(ExchangeTypesSupported.Direct)));

                    exchangeCreated[deadLetterExchange] = deadLetterExchange;
                }

                wrapper.QueueDeclare(channel, deadLetterQueue, retryExchange.HasValue()
                    ? new Dictionary<string, object>
                    {
                        [Constants.HeaderDeadLetterExchange] = retryExchange,
                        [Constants.HeaderDeadLetterRoutingKey] = queue.DeadLetterRoutingKey.EmptyAlternative(queue.Name)
                    }
                    : null);

                wrapper.QueueBind(channel, deadLetterQueue, deadLetterExchange,
                    queue.DeadLetterRoutingKey.EmptyAlternative(queue.Name), null);
                
                

                if (retryExchange.HasValue())
                {
                    if (!exchangeCreated.ContainsKey(retryExchange))
                    {
                        wrapper.ExchangeDeclare(channel, retryExchange, queue.RetryExchangeType.EmptyAlternative(
                            settings.DefaultRetryExchangeType.EmptyAlternative(ExchangeTypesSupported.Direct)));

                        exchangeCreated[retryExchange] = retryExchange;
                    }

                    wrapper.QueueBind(channel, queue.Name, retryExchange,
                        queue.DeadLetterRoutingKey.EmptyAlternative(queue.Name), null);
                }
            }

            var queueBinding = queue.Bindings ?? [];

            if (exchangeType == ExchangeTypesSupported.Headers)
            {
                if (queueBinding.Length == 0)
                {
                    wrapper.QueueBind(channel, queue.Name, exchange, string.Empty, null);
                }
                else
                {
                    SetupHeaderBindings(queueBinding, channel, queue, exchange);
                }
            }
            else if (exchangeType != ExchangeTypesSupported.Fanout)
            {
                if (queueBinding.Length == 0)
                {
                    wrapper.QueueBind(channel, queue.Name, exchange, string.Empty, null);
                }
                else
                {
                    foreach (var binding in queueBinding)
                    {
                        if (binding.RoutingKey.HasValue())
                        {
                            wrapper.QueueBind(channel, queue.Name, exchange, binding.RoutingKey, null);
                        }
                        else
                        {
                            wrapper.QueueBind(channel, queue.Name, exchange, string.Empty, null);
                        }
                    }
                }
            }
            else
            {
                wrapper.QueueBind(channel, queue.Name, exchange, string.Empty, null);
            }

            result.Add(new QueueInfo
            {
                Exchange = exchange,
                ExchangeType = exchangeType,
                Name = queue.Name,
                DeadLetterExchange = deadLetterExchange,
                RetryExchange = retryExchange,
                PrefetchCount = queue.PrefetchCount ?? settings.DefaultPrefetchCount ?? 5,
                DefaultRetryAfterInSeconds = queue.DefaultRetryAfterInSeconds
            });
        }

        return result.ToArray();
    }

    private void SetupHeaderBindings(QueueBinding[] queueBinding, IModel channel, QueueSettings queue, string exchange)
    {
        foreach (var binding in queueBinding)
        {
            var bindingArgs = new Dictionary<string, object>();

            var headersToBind = binding.Headers;

            if (headersToBind == null || headersToBind.Count == 0) continue;

            foreach (var headerToBind in headersToBind)
            {
                bindingArgs[headerToBind.Key] = headerToBind.Value;
            }

            if (binding.MatchAny ?? false)
            {
                bindingArgs[Constants.HeaderMatch] = "any";
            }
            else
            {
                bindingArgs[Constants.HeaderMatch] = "all";
            }

            wrapper.QueueBind(channel, queue.Name, exchange, string.Empty, bindingArgs);
        }
    }

    private string DefaultDeadLetterExchange(string exchange) => $"{exchange}-dlx";
    private string DefaultDeadLetterQueue(string queueName) => $"{queueName}-dlq";
    private string DefaultRetryExchange(string exchange) => $"{exchange}-rtx";
}

public interface IQueueSettings
{
    public string DefaultExchange { get; }

    /// <summary>
    /// Valid values are null, fanout, direct, topic and headers
    /// </summary>
    public string? DefaultExchangeType { get; }

    public string? DefaultDeadLetterExchange { get; }
    public string? DefaultDeadLetterExchangeType { get; }
    public string? DefaultRetryExchange { get; }
    public string? DefaultRetryExchangeType { get; }

    public int? DefaultPrefetchCount { get; }

    QueueSettings[]? Queues { get; }
    public string? AppId { get; }
}

public record QueueInfo
{
    public string Name { get; set; } = String.Empty;
    public string Exchange { get; set; } = String.Empty;
    public string ExchangeType { get; set; } = String.Empty;

    /// <summary>
    /// Optional, when empty following name used `{queueName}-dlx`
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    public string? RetryExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public int? DefaultRetryAfterInSeconds { get; set; }
    public int? PrefetchCount { get; set; }
}
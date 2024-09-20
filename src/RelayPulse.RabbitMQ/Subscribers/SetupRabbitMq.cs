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

        var channel = connectionInstance.Get().CreateModel();

        foreach (var queue in settings.Queues)
        {
            var exchange = queue.Exchange.TryPickNonEmpty(settings.DefaultExchange) ?? string.Empty;

            if (!exchange.HasValue()) throw new Exception("Exchange name cannot be empty");

            var exchangeType = queue.ExchangeType.TryPickNonEmpty(settings.DefaultExchangeType)
                .EmptyAlternative(ExchangeTypesSupported.Direct);

            var deadLetterExchange = queue.DeadLetterDisabled
                ? null
                : queue.DeadLetterExchange.EmptyAlternative(DefaultDeadLetterExchange(exchange));
            var deadLetterQueue = queue.DeadLetterDisabled
                ? null
                : queue.DeadLetterQueue.EmptyAlternative(DefaultDeadLetterQueue(queue.Name));


            var retryExchange = queue.RetryDisabled
                ? null
                : queue.RetryExchange.EmptyAlternative(DefaultRetryExchange(exchange));

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
                queueArgs[Constants.HeaderDeadLetterRoutingKey] = queue.Name;
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
                    wrapper.ExchangeDeclare(channel, deadLetterExchange, ExchangeTypesSupported.Direct);

                    exchangeCreated[deadLetterExchange] = deadLetterExchange;
                }

                wrapper.QueueDeclare(channel, deadLetterQueue, retryExchange.HasValue()
                    ? new Dictionary<string, object>
                    {
                        [Constants.HeaderDeadLetterExchange] = retryExchange,
                        [Constants.HeaderDeadLetterRoutingKey] = queue.Name
                    }
                    : null);

                wrapper.QueueBind(channel, deadLetterQueue, deadLetterExchange, queue.Name, null);
            }

            if (retryExchange.HasValue())
            {
                if (!exchangeCreated.ContainsKey(retryExchange))
                {
                    wrapper.ExchangeDeclare(channel, retryExchange, ExchangeTypesSupported.Direct);

                    exchangeCreated[retryExchange] = retryExchange;
                }
                
                wrapper.QueueBind(channel, queue.Name, retryExchange, queue.Name, null);
            }

            var queueBinding = queue.Bindings ?? [];

            if (exchangeType == ExchangeTypesSupported.Headers)
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
            else if (exchangeType != ExchangeTypesSupported.Fanout)
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
                PrefetchCount = queue.PrefetchCount ?? settings.DefaultPrefetchCount,
                DefaultRetryAfterInSeconds = queue.DefaultRetryAfterInSeconds
            });
        }

        return result.ToArray();
    }

    private string DefaultDeadLetterExchange(string queueName) => $"{queueName}-dlx";
    private string DefaultDeadLetterQueue(string queueName) => $"{queueName}-dlq";
    private string DefaultRetryExchange(string queueName) => $"{queueName}-rtx";
}

public interface IQueueSettings
{
    public string DefaultExchange { get; }

    /// <summary>
    /// Valid values are null, fanout, direct, topic and headers
    /// </summary>
    public string? DefaultExchangeType { get; }

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
    public int? DefaultRetryAfterInSeconds { get; set; }
    public int? PrefetchCount { get; set; }
}
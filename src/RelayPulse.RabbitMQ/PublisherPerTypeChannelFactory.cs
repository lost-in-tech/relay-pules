using System.Collections.Concurrent;
using RabbitMQ.Client;

namespace RelayPulse.RabbitMQ;

internal interface IChannelFactory : IDisposable
{
    IModel GetOrCreate(string key);
    bool IsApplicable(string key, bool forPublisher);
}

internal sealed class PublisherDefaultChannelFactory(
    IPublisherChannelSettings settings,
    IRabbitMqConnectionInstance connection) 
    : IChannelFactory
{
    private readonly object _lock = new();
    private IModel? _channel;

    public IModel GetOrCreate(string key)
    {
        if (_channel is { IsClosed: false }) return _channel;

        lock (_lock)
        {
            if (_channel is { IsClosed: false }) return _channel;

            try
            {
                _channel?.Close();
            }
            catch { /* ignore */ }
            _channel?.Dispose();

            _channel = connection.Get().CreateModel();
            return _channel;
        }
    }

    public bool IsApplicable(string key, bool forPublisher)
    {
        return forPublisher && settings.UseChannelPerType is null or false;
    }
    
    public void Dispose()
    {
        if (_channel is { IsClosed: false })
        {
            _channel.Close();
        }
        _channel?.Dispose();
    }

}

internal sealed class PublisherPerTypeChannelFactory(
    IRabbitMqConnectionInstance connectionInstance,
    IPublisherChannelSettings settings) 
    : IChannelFactory
{
    private readonly ConcurrentDictionary<string,Lazy<IModel>> _source = new();
    
    public IModel GetOrCreate(string key)
    {
        var lazyChannel = _source.GetOrAdd(key, _ => new Lazy<IModel>(() =>  connectionInstance.Get().CreateModel()));

        var channel = lazyChannel.Value;
        
        if (channel.IsClosed)
        {
            var newChannel = _source.AddOrUpdate(key, 
                _ => new Lazy<IModel>(() => connectionInstance.Get().CreateModel()),
                (_, cnl) => cnl.Value.IsClosed ? new Lazy<IModel>(() => connectionInstance.Get().CreateModel()) : cnl);

            return newChannel.Value;
        }
        
        return channel;
    }

    public bool IsApplicable(string key, bool forPublisher)
    {
        return forPublisher && (settings.UseChannelPerType ?? false);
    }

    public void Dispose()
    {
        foreach (var lazyChannel in _source)
        {
            var channel = lazyChannel.Value;
            
            if (!channel.Value.IsClosed)
            {
                channel.Value.Close();
                channel.Value.Dispose();
            }
        }
    }
}

public interface IPublisherChannelSettings
{
    public bool? UseChannelPerType { get; }
}
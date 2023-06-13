using Caching;
using Hazelcast;
using Hazelcast.Configuration;
using Hazelcast.DistributedObjects;
using Microsoft.Extensions.Caching.Distributed;

internal class HazelcastCache : IDistributedCache, IAsyncDisposable
{
    private readonly HazelcastOptions? _hazelcastOptions;
    private readonly string _mapName;
    private bool _disposed;
    private IHMap<string, byte[]>? _map;
    private readonly SemaphoreSlim _clientLock = new(1, 1);
    private readonly HazelcastFailoverOptions? _hazelcastFailoverOptions;
    private IHazelcastClient? _client;

    public HazelcastCache(HazelcastOptions hazelcastOptions, HazelcastCacheOptions cacheOptions)
    {
        if (cacheOptions is null) throw new ArgumentNullException(nameof(cacheOptions));
        _hazelcastOptions = hazelcastOptions ?? throw new ArgumentNullException(nameof(hazelcastOptions));
        _mapName = CreateMapName(cacheOptions.CacheUniqueIdentifier);
    }

    public HazelcastCache(HazelcastFailoverOptions hazelcastFailoverOptions, HazelcastCacheOptions cacheOptions)
    {
        if (cacheOptions is null) throw new ArgumentNullException(nameof(cacheOptions));
        _hazelcastFailoverOptions = hazelcastFailoverOptions ?? throw new ArgumentNullException(nameof(hazelcastFailoverOptions));
        _mapName = CreateMapName(cacheOptions.CacheUniqueIdentifier);
    }

    private static string CreateMapName(string? cacheId) => cacheId ?? "distributed_cache";

    private async Task ConnectAsync(CancellationToken token = default)
    {
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
        token.ThrowIfCancellationRequested();

        if (_map is not null) return;

        await _clientLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_map is not null) return;

            if (_hazelcastOptions is not null)
            {
                _client = await HazelcastClientFactory.StartNewClientAsync(_hazelcastOptions, token).ConfigureAwait(false);
            }
            else if (_hazelcastFailoverOptions is not null)
            {
                _client = await HazelcastClientFactory.StartNewFailoverClientAsync(_hazelcastFailoverOptions, token).ConfigureAwait(false);
            }
            else
            {
                throw new ConfigurationException("Hazelcast client options or failover client options must be provided.");
            }

            _map = await _client.GetMapAsync<string, byte[]>(_mapName).ConfigureAwait(false);
        }
        finally
        {
            _clientLock.Release();
        }
    }

    public byte[]? Get(string key) => GetAsync(key).GetAwaiter().GetResult();

    public Task<byte[]?> GetAsync(string key, CancellationToken token = default) => GetAndRefreshAsync(key, getData: true, token);

    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) => SetAsync(key, value, options).GetAwaiter().GetResult();

    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));
        if (value is null) throw new ArgumentNullException(nameof(value));
        if (options is null) throw new ArgumentNullException(nameof(options));

        token.ThrowIfCancellationRequested();
        await ConnectAsync(token).ConfigureAwait(false);

        var maxIdle = options.SlidingExpiration ?? TimeSpan.Zero;
        var timeToLive = options.AbsoluteExpirationRelativeToNow ?? (options.AbsoluteExpiration.HasValue ? options.AbsoluteExpiration.Value - DateTimeOffset.UtcNow : TimeSpan.Zero);

        if (maxIdle < TimeSpan.Zero || timeToLive < TimeSpan.Zero)
        {
            throw new ArgumentException("Options produce negative max-idle or time-to-live.", nameof(options));
        }
        await _map.SetAsync(key, value, timeToLive, maxIdle).ConfigureAwait(false);
    }

    public void Refresh(string key) => GetAndRefreshAsync(key, getData: false).GetAwaiter().GetResult();

    public Task RefreshAsync(string key, CancellationToken token = default) => GetAndRefreshAsync(key, getData: true, token);

    private async Task<byte[]?> GetAndRefreshAsync(string key, bool getData, CancellationToken token = default)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        token.ThrowIfCancellationRequested();
        await ConnectAsync(token).ConfigureAwait(false);

        if (getData) return await _map.GetAsync(key).ConfigureAwait(false);

        await _map.ContainsKeyAsync(key).ConfigureAwait(false);
        return default;
    }

    public void Remove(string key) => RemoveAsync(key).GetAwaiter().GetResult();

    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        if (key is null) throw new ArgumentNullException(nameof(key));

        await ConnectAsync(token).ConfigureAwait(false);
        await _map.DeleteAsync(key).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_map is not null) await _map.DisposeAsync().ConfigureAwait(false);
        if (_client is not null) await _client.DisposeAsync().ConfigureAwait(false);
    }
}

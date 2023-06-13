using Hazelcast;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

namespace Caching
{
    internal static class ServiceCollectionExtensions
    {
        internal static IServiceCollection AddHazelcastCache(this IServiceCollection services, HazelcastOptions hazelcastOptions, HazelcastCacheOptions cacheOptions)
        {
            return services.AddSingleton<IDistributedCache>(_ => new HazelcastCache(hazelcastOptions, cacheOptions));
        }

        internal static IServiceCollection AddHazelcastCache(this IServiceCollection services, HazelcastFailoverOptions hazelcastOptions, HazelcastCacheOptions cacheOptions)
        {
            return services.AddSingleton<IDistributedCache>(_ => new HazelcastCache(hazelcastOptions, cacheOptions));
        }

        internal static IServiceCollection AddHazelcastCache(this IServiceCollection services, bool withFailover = false)
        {
            if (withFailover)
            {
                return services.AddSingleton<IDistributedCache, ProvidedHazelcastCacheWithFailover>();
            }
            return services.AddSingleton<IDistributedCache, ProvidedHazelcastCache>();
        }
    }
}

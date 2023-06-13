using Hazelcast;
using Microsoft.Extensions.Options;

namespace Caching
{
    internal sealed class ProvidedHazelcastCache : HazelcastCache
    {
        public ProvidedHazelcastCache(IOptions<HazelcastOptions> hazelcastOptions, IOptions<HazelcastCacheOptions> hazelcastCacheOptions) : base(hazelcastOptions.SafeValue(), hazelcastCacheOptions.SafeValue())
        {
        }
    }
}

using Hazelcast;
using Microsoft.Extensions.Options;

namespace Caching
{
    internal sealed class ProvidedHazelcastCacheWithFailover : HazelcastCache
    {
        public ProvidedHazelcastCacheWithFailover(IOptions<HazelcastFailoverOptions> hazelcastFailoverOptions, IOptions<HazelcastCacheOptions> hazelcastCacheOptions) : base(hazelcastFailoverOptions.SafeValue(), hazelcastCacheOptions.SafeValue())
        {
        }
    }
}

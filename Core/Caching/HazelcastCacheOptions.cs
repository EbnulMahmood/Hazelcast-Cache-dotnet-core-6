using Microsoft.Extensions.Options;

namespace Caching
{
    internal sealed class HazelcastCacheOptions : IOptions<HazelcastCacheOptions>
    {
        public string? CacheUniqueIdentifier { get; set; }

        HazelcastCacheOptions IOptions<HazelcastCacheOptions>.Value => this;
    }
}

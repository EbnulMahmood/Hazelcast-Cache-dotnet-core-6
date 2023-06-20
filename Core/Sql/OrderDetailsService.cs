using Hazelcast;
using Hazelcast.Core;

namespace Sql
{
    public interface IOrderDetailsService
    {
        Task<int> CreateOrderDetailsMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default);
    }

    internal sealed class OrderDetailsService : IOrderDetailsService
    {
        private readonly HazelcastOptions _options;
        private readonly string _mapName;

        public OrderDetailsService(HazelcastOptions options, string mapName)
        {
            _options = options;
            _mapName = mapName;
        }

        public async Task<int> CreateOrderDetailsMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                await client.Sql.ExecuteCommandAsync($@"
CREATE OR REPLACE MAPPING 
{map.Name} (
__key INT,
Id INT,
OrderId INT,
ProductId INT,
IsActive BOOLEAN)
TYPE IMap OPTIONS ('keyFormat'='int', 'valueFormat'='json-flat')", cancellationToken: token).ConfigureAwait(false);

                if (isSetAll)
                {
                    await map.SetAllAsync(entries).ConfigureAwait(false);
                }
                else
                {
                    foreach (var (key, jsonObj) in entries)
                    {
                        {
                            await map.SetAsync(key, jsonObj).ConfigureAwait(false);
                        }
                    }
                }

                var count = await map.GetSizeAsync().ConfigureAwait(false);

                await map.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return count;
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

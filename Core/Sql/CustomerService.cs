using Document;
using Hazelcast;
using Hazelcast.Core;
using Hazelcast.DistributedObjects;
using System.Text.Json;

namespace Sql
{
    public interface ICustomerService
    {
        Task<IHMap<int, HazelcastJsonValue>> CreateCustomerMapAsync(IDictionary<int, Customer> entries, string mapName = "", bool useSql = false, CancellationToken token = default);
    }
    internal sealed class CustomerService : ICustomerService
    {
        private readonly HazelcastOptions _options;

        public CustomerService(HazelcastOptions options)
        {
            _options = options;
        }

        public async Task<IHMap<int, HazelcastJsonValue>> CreateCustomerMapAsync(IDictionary<int, Customer> entries, string mapName = "", bool useSql = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(mapName))
                {
                    mapName = GenerateMapName();
                }
                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(mapName).ConfigureAwait(false);

                await client.Sql.ExecuteCommandAsync($@"CREATE OR REPLACE MAPPING {map.Name} TYPE IMap OPTIONS ('keyFormat'='int', 'valueFormat'='json')", cancellationToken: token).ConfigureAwait(false);

                foreach (var (key, obj) in entries)
                {
                    string jsonObject = JsonSerializer.Serialize(obj);
                    if (useSql)
                    {
                        //var parameters = new List<object> { obj.Id, jsonObject }.ToArray();
                        //await client.Sql.ExecuteCommandAsync($@"INSERT INTO {map.Name} VALUES (?,?)", parameters: parameters, cancellationToken: token).ConfigureAwait(false);
                    }
                    else
                    {
                        await map.PutAsync(key, new HazelcastJsonValue(jsonObject)).ConfigureAwait(false);
                    }
                }

                return map;
            }
            catch (Exception)
            {

                throw;
            }
        }

        private static string GenerateMapName()
            => new($"{Guid.NewGuid():N}".Select(c => char.IsDigit(c) ? (char)(c + 'g' - '1') : c).ToArray());
    }
}

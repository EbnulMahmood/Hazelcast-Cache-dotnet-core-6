using Document;
using Hazelcast;
using Hazelcast.Core;
using Hazelcast.DistributedObjects;
using Hazelcast.Sql;
using System.Text.Json;

namespace Sql
{
    public interface ICustomerService
    {
        Task<List<Customer>> LoadCustomerAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default);
        Task<IHMap<int, HazelcastJsonValue>> CreateCustomerMapAsync(IDictionary<int, Customer> entries, bool useSql = false, CancellationToken token = default);
    }
    internal sealed class CustomerService : ICustomerService
    {
        private readonly HazelcastOptions _options;
        private readonly string _mapName;

        public CustomerService(HazelcastOptions options, string mapName)
        {
            _options = options;
            _mapName = mapName;
        }

        public async Task<List<Customer>> LoadCustomerAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                if (isInMemory)
                {
                    var customers = new List<Customer>();
                    var result = await map.GetEntriesAsync().ConfigureAwait(false);
                    foreach (var (key, jsonObj) in result)
                    {
                        var obj = JsonSerializer.Deserialize<Customer>(jsonObj.ToString());
                        customers.Add(obj);
                    }
                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return customers;
                }
                else if (isSqlQuery)
                {
                    await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
*
FROM {map.Name}", cancellationToken: token).ConfigureAwait(false);

                    var customers = await GetJsonTOEntityListAsync<Customer>(result, token).ConfigureAwait(false);

                    await result.DisposeAsync().ConfigureAwait(false);
                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return customers;
                }
                else
                {
                    var query = await map.AsAsyncQueryable()
                        .ToListAsync(cancellationToken: token).ConfigureAwait(false);

                    var customers = new List<Customer>();
                    foreach (var (key, jsonObj) in query)
                    {
                        var obj = JsonSerializer.Deserialize<Customer>(jsonObj.ToString());
                        customers.Add(obj);
                    }

                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return customers;
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

        private static async Task<List<TEntity>> GetJsonTOEntityListAsync<TEntity>(ISqlQueryResult result, CancellationToken token = default)
        {
            var objList = new List<TEntity>();
            var enumerator = result.GetAsyncEnumerator(cancellationToken: token);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var row = enumerator.Current;
                var jsonObj = row.GetValue<HazelcastJsonValue>();
                var obj = JsonSerializer.Deserialize<TEntity>(jsonObj.ToString());
                if (obj is not null)
                    objList.Add(obj);
            }

            return objList;
        }

        public async Task<IHMap<int, HazelcastJsonValue>> CreateCustomerMapAsync(IDictionary<int, Customer> entries, bool useSql = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                await client.Sql.ExecuteCommandAsync($@"CREATE OR REPLACE MAPPING {map.Name} TYPE IMap OPTIONS ('keyFormat'='int', 'valueFormat'='json')", cancellationToken: token).ConfigureAwait(false);

                foreach (var (key, obj) in entries)
                {
                    string jsonObject = JsonSerializer.Serialize(obj);
                    if (useSql)
                    {
                        //var parameters = new List<object> { obj.Id, jsonObject }.ToArray();
                        //await client.Sql.ExecuteCommandAsync($@"INSERT INTO {map.Name} VALUES (?,?)", parameters: new { obj.Id, jsonObject }, cancellationToken: token).ConfigureAwait(false);
                    }
                    else
                    {
                        await map.SetAsync(key, new HazelcastJsonValue(jsonObject)).ConfigureAwait(false);
                    }
                }

                await client.DisposeAsync().ConfigureAwait(false);
                await map.DisposeAsync().ConfigureAwait(false);

                return map;
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

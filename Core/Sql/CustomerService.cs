using Document;
using Hazelcast;
using Hazelcast.Core;
using Hazelcast.Models;
using Hazelcast.Sql;
using System.Text.Json;

namespace Sql
{
    public interface ICustomerService
    {
        Task<string> LoadCustomerAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default);
        Task<int> CreateCustomerMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default);
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

        public async Task<string> LoadCustomerAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                if (isInMemory)
                {
                    var customers = new List<Customer>();
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    //var result = await map.GetEntriesAsync().ConfigureAwait(false);

                    //var keys = new List<int>();
                    //for (int i = 1; i <= 5000000; i++)
                    //{
                    //    keys.Add(i);
                    //}
                    //var result = await map.GetAllAsync(keys).ConfigureAwait(false);
                    //var r = result.ToList().Select(x => x.Value);

                    var result = await map.GetValuesAsync().ConfigureAwait(false);
                    watch.Stop();
                    var objList = result.ToList().Select(x => JsonSerializer.Deserialize<Customer>(x.Value.ToString()));
                    var count = result.Count;

                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
                }
                else if (isSqlQuery)
                {
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
Id
,Name
,Address
,CreatedAt
FROM {map.Name}", cancellationToken: token).ConfigureAwait(false);

                    var customerList = await result.Select(row =>
                        new Customer
                        {
                            Id = row.GetColumn<int>("Id"),
                            Name = row.GetColumn<string>("Name"),
                            Address = row.GetColumn<string>("Address"),
                            CreatedAt = (DateTimeOffset)row.GetColumn<HOffsetDateTime>("CreatedAt"),
                        }
                    ).ToListAsync(cancellationToken: token).ConfigureAwait(false);
                    watch.Stop();

                    var count = customerList.Count;
                    //var customers = await GetJsonTOEntityListAsync<Customer>(result, token).ConfigureAwait(false);

                    await result.DisposeAsync().ConfigureAwait(false);
                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
                }
                else
                {
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    var query = await map.AsAsyncQueryable()
                        .ToListAsync(cancellationToken: token).ConfigureAwait(false);

                    watch.Stop();
                    var objList = query.ToList().Select(x => JsonSerializer.Deserialize<Customer>(x.Value.ToString()));
                    var count = objList.Count();

                    var customers = new List<Customer>();
                    foreach (var (_, jsonObj) in query)
                    {
                        var obj = JsonSerializer.Deserialize<Customer>(jsonObj.ToString());
                        customers.Add(obj);
                    }

                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
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

        public async Task<int> CreateCustomerMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default)
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
Name VARCHAR,
Address VARCHAR,
CreatedAt TIMESTAMP WITH TIME ZONE)
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

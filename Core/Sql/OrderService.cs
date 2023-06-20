using Document;
using Hazelcast;
using Hazelcast.Core;
using Hazelcast.Models;

namespace Sql
{
    public interface IOrderService
    {
        Task<string> LoadCustomerOrderAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default);
        Task<int> CreateOrderMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default);
    }

    internal sealed class OrderService : IOrderService
    {
        private readonly HazelcastOptions _options;
        private readonly string _mapName;

        public OrderService(HazelcastOptions options, string mapName)
        {
            _options = options;
            _mapName = mapName;
        }

        public async Task<string> LoadCustomerOrderAsync(bool isSqlQuery = true, bool isInMemory = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                if (isInMemory)
                {
                    //var customerOrders = new List<CustomerOrder>();
                    //var watch = System.Diagnostics.Stopwatch.StartNew();
                    //var result = await map.GetEntriesAsync().ConfigureAwait(false);

                    //var keys = new List<int>();
                    //for (int i = 1; i <= 5000000; i++)
                    //{
                    //    keys.Add(i);
                    //}
                    //var result = await map.GetAllAsync(keys).ConfigureAwait(false);
                    //var r = result.ToList().Select(x => x.Value);

                    //var result = await map.GetValuesAsync().ConfigureAwait(false);
                    //watch.Stop();
                    //var objList = result.ToList().Select(x => JsonSerializer.Deserialize<CustomerOrder>(x.Value.ToString()));
                    //var count = result.Count;

                    //await map.DisposeAsync().ConfigureAwait(false);
                    //await client.DisposeAsync().ConfigureAwait(false);

                    //return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
                }
                else if (isSqlQuery)
                {
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT
co.Id
,co.Quantity
,co.Price
,co.CreatedAt AS OrderDate
,c.Name
,c.Address
,c.CreatedAt AS CustomerCretatedAt
FROM customerOrder AS co
INNER JOIN customer AS c
ON co.CustomerId = c.__key", cancellationToken: token).ConfigureAwait(false);

                    var customerList = await result.Select(row =>
                        new CustomerOrder
                        {
                            Id = row.GetColumn<int>("Id"),
                            Quantity = row.GetColumn<int>("Quantity"),
                            Price = row.GetColumn<double>("Price"),
                            OrderDate = (DateTimeOffset)row.GetColumn<HOffsetDateTime>("OrderDate"),
                            Name = row.GetColumn<string>("Name"),
                            Address = row.GetColumn<string>("Address"),
                            CustomerCretatedAt = (DateTimeOffset)row.GetColumn<HOffsetDateTime>("CustomerCretatedAt"),
                        }
                    ).ToListAsync(cancellationToken: token).ConfigureAwait(false);
                    watch.Stop();

                    var count = customerList.Count;

                    await result.DisposeAsync().ConfigureAwait(false);
                    await map.DisposeAsync().ConfigureAwait(false);
                    await client.DisposeAsync().ConfigureAwait(false);

                    return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
                }
                else
                {
                    //var watch = System.Diagnostics.Stopwatch.StartNew();
                    //var query = await map.AsAsyncQueryable()
                    //    .ToListAsync(cancellationToken: token).ConfigureAwait(false);

                    //watch.Stop();
                    //var objList = query.ToList().Select(x => JsonSerializer.Deserialize<Customer>(x.Value.ToString()));
                    //var count = objList.Count();

                    //var customers = new List<Customer>();
                    //foreach (var (_, jsonObj) in query)
                    //{
                    //    var obj = JsonSerializer.Deserialize<Customer>(jsonObj.ToString());
                    //    customers.Add(obj);
                    //}

                    //await map.DisposeAsync().ConfigureAwait(false);
                    //await client.DisposeAsync().ConfigureAwait(false);

                    //return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
                }
                return "";
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> CreateOrderMapAsync(IDictionary<int, HazelcastJsonValue> entries, bool isSetAll = false, CancellationToken token = default)
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
CustomerId INT,
Quantity INT,
Price DOUBLE,
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

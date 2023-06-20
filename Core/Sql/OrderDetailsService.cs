using Document;
using Hazelcast;
using Hazelcast.Core;
using Hazelcast.Models;

namespace Sql
{
    public interface IOrderDetailsService
    {
        Task<string> LoadOrderDetailsWithCustomerOrderAsync(CancellationToken token = default);
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

        public async Task<string> LoadOrderDetailsWithCustomerOrderAsync(CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var map = await client.GetMapAsync<int, HazelcastJsonValue>(_mapName).ConfigureAwait(false);

                var watch = System.Diagnostics.Stopwatch.StartNew();
                await using var result = await client.Sql.ExecuteQueryAsync($@"
SELECT 
od.Id
,od.IsActive
,od.ProductId
,co.Quantity
,co.Price
,co.CreatedAt AS OrderDate
,c.Name
,c.Address
,c.CreatedAt AS CustomerCretatedAt
FROM orderDetails AS od
INNER JOIN customerOrder AS co
ON od.OrderId = co.__key
INNER JOIN customer AS c
ON c.__key = co.CustomerId", cancellationToken: token).ConfigureAwait(false);

                var customerList = await result.Select(row =>
                    new OrderDetailsWithCustomerOrder
                    {
                        Id = row.GetColumn<int>("Id"),
                        IsActive = row.GetColumn<bool>("IsActive"),
                        ProductId = row.GetColumn<int>("ProductId"),
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
            catch (Exception)
            {

                throw;
            }
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

using Document;
using Hazelcast;

namespace Sql
{
    public interface IProductService
    {
        Task<string> LoadProductListAsync(CancellationToken token = default);
        Task<int> CreateProductListAsync(List<Product> entities, bool isAddAll = false, CancellationToken token = default);
    }

    internal sealed class ProductService : IProductService
    {
        private readonly HazelcastOptions _options;
        private readonly string _listName;

        public ProductService(HazelcastOptions options, string listName)
        {
            _options = options;
            _listName = listName;
        }

        public async Task<string> LoadProductListAsync(CancellationToken token = default)
        {
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var list = await client.GetListAsync<Product>(_listName).ConfigureAwait(false);
                watch.Stop();
                var count = await list.CountAsync(cancellationToken: token).ConfigureAwait(false);

                var products = await list.ToListAsync(cancellationToken: token).ConfigureAwait(false);

                await list.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return $"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes";
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> CreateProductListAsync(List<Product> entities, bool isAddAll = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var list = await client.GetListAsync<Product>(_listName).ConfigureAwait(false);

                if (isAddAll)
                {
                    await list.AddAllAsync(0, entities).ConfigureAwait(false);
                    //await task.WaitAsync(token).ConfigureAwait(false);
                }
                else
                {
                    foreach (var entity in entities)
                    {
                        await list.AddAsync(entity).ConfigureAwait(false);
                    }
                }

                var count = await list.CountAsync(cancellationToken: token).ConfigureAwait(false);

                await list.DisposeAsync().ConfigureAwait(false);
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

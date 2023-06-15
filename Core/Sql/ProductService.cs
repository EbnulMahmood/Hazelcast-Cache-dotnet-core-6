using Document;
using Hazelcast;

namespace Sql
{
    public interface IProductService
    {
        Task<List<Product>> LoadProductListAsync(CancellationToken token = default);
        Task CreateProductListAsync(List<Product> entities, bool isAddAll = false, CancellationToken token = default);
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

        public async Task<List<Product>> LoadProductListAsync(CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var list = await client.GetListAsync<Product>(_listName).ConfigureAwait(false);

                var products = await list.ToListAsync(cancellationToken: token).ConfigureAwait(false);

                await list.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);

                return products;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task CreateProductListAsync(List<Product> entities, bool isAddAll = false, CancellationToken token = default)
        {
            try
            {
                await using var client = await HazelcastClientFactory.StartNewClientAsync(_options, cancellationToken: token).ConfigureAwait(false);

                await using var list = await client.GetListAsync<Product>(_listName).ConfigureAwait(false);

                if (isAddAll)
                {
                    var task = list.AddAllAsync(0, entities);
                    await task.WaitAsync(token).ConfigureAwait(false);
                }
                else
                {
                    foreach (var entity in entities)
                    {
                        await list.AddAsync(entity).ConfigureAwait(false);
                    }
                }

                await list.DisposeAsync().ConfigureAwait(false);
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

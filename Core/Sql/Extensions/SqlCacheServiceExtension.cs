using Hazelcast;
using Microsoft.Extensions.DependencyInjection;

namespace Sql.Extensions
{
    public static class SqlCacheServiceExtension
    {
        public static void AddCacheService(this IServiceCollection services, HazelcastOptions hazelcastOptions)
        {
            services.AddSingleton<ICustomerService, CustomerService>(_ => new CustomerService(hazelcastOptions, "customer"));
            services.AddSingleton<IProductService, ProductService>(_ => new ProductService(hazelcastOptions, "product"));
            services.AddSingleton<IOrderService, OrderService>(_ => new OrderService(hazelcastOptions, "customerOrder"));
        }
    }
}

using Document;
using HazelcastCacheAPI.Helper;
using Microsoft.AspNetCore.Mvc;
using Sql;

namespace HazelcastCacheAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductsController : ControllerBase
    {
        private readonly IProductService _productService;

        public ProductsController(IProductService productService)
        {
            _productService = productService;
        }

        [HttpGet]
        [Route("/products")]
        public async Task<IActionResult> LoadProduct(CancellationToken token = default)
        {
            try
            {
                var products = await _productService.LoadProductListAsync(token).ConfigureAwait(false);
                return Ok(products);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpPost]
        [Route("/data/seed-products")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var entities = new List<Product>();
                for (var i = 1; i <= Constants.fiveMillion; i++)
                {
                    entities.Add(new Product
                    {
                        Id = i,
                        Name = $"Product_{i}",
                        Price = i,
                        CreatedAt = DateTimeOffset.UtcNow
                    });
                }
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var count = await _productService.CreateProductListAsync(entities, isAddAll:true, token: token).ConfigureAwait(false);
                watch.Stop();

                return Ok($"{count} Records Load Time: {watch.ElapsedMilliseconds} milliseconds, {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalSeconds} seconds and {TimeSpan.FromMilliseconds(watch.ElapsedMilliseconds).TotalMinutes} minutes");
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

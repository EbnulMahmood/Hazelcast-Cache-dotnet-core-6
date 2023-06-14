using Document;
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
                for (var i = 1; i <= 10; i++)
                {
                    entities.Add(new Product
                    {
                        Id = i,
                        Name = $"Product_{i}",
                        Price = i,
                        CreatedAt = DateTimeOffset.UtcNow
                    });
                }
                await _productService.CreateProductListAsync(entities, true, token: token).ConfigureAwait(false);

                return Ok(entities);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

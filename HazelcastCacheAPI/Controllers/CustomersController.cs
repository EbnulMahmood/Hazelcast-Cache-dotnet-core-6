using Document;
using Microsoft.AspNetCore.Mvc;
using Sql;

namespace HazelcastCacheAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CustomersController : ControllerBase
    {
        private readonly ICustomerService _customerService;

        public CustomersController(ICustomerService customerService)
        {
            _customerService = customerService;
        }

        [HttpGet]
        [Route("/customers")]
        public async Task<IActionResult> LoadCustomer(CancellationToken token = default) 
        {
            try
            {
                var customers = await _customerService.LoadCustomerAsync(true, token: token).ConfigureAwait(false);
                return Ok(customers);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpGet]
        [Route("/customers/linq")]
        public async Task<IActionResult> LoadCustomerLINQ(CancellationToken token = default)
        {
            try
            {
                var customers = await _customerService.LoadCustomerAsync(false, token: token).ConfigureAwait(false);
                return Ok(customers);
            }
            catch (Exception)
            {

                throw;
            }
        }
        [HttpGet]
        [Route("/customers/memory")]
        public async Task<IActionResult> LoadCustomerMemory(CancellationToken token = default)
        {
            try
            {
                var customers = await _customerService.LoadCustomerAsync(false, true, token: token).ConfigureAwait(false);
                return Ok(customers);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpPost]
        [Route("/data/seed-customers")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var entities = new Dictionary<int, Customer>();
                for (var i = 1; i <= 10; i++)
                {
                    entities[i] = new Customer
                    {
                        Id = i,
                        Name = $"Customer_{i}",
                        Address = $"Address_{i}",
                        CreatedAt = DateTimeOffset.UtcNow
                    };
                }
                await _customerService.CreateCustomerMapAsync(entities, useSql: false, token: token).ConfigureAwait(false);

                return Ok(entities);
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}

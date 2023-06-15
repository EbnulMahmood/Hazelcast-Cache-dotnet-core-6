using Document;
using Hazelcast.Core;
using HazelcastCacheAPI.Helper;
using Microsoft.AspNetCore.Mvc;
using Sql;
using System.Text.Json;

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
                var entities = new Dictionary<int, HazelcastJsonValue>();
                for (var i = 1; i <= Constants.fiveMillion; i++)
                {
                    var obj = new Customer
                    {
                        Id = i,
                        Name = $"Customer_{i}",
                        Address = $"Address_{i}",
                        CreatedAt = DateTimeOffset.UtcNow
                    };
                    //entities[i] = obj;
                    string jsonObject = JsonSerializer.Serialize(obj);
                    entities[i] = new HazelcastJsonValue(jsonObject);
                }
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var count = await _customerService.CreateCustomerMapAsync(entities, isSetAll: true, token: token).ConfigureAwait(false);
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

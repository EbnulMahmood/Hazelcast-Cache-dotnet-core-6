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
    public class OrdersController : ControllerBase
    {
        private readonly IOrderService _orderService;

        public OrdersController(IOrderService orderService)
        {
            _orderService = orderService;
        }

        [HttpGet]
        [Route("/customer-orders")]
        public async Task<IActionResult> LoadCustomerOrder(CancellationToken token = default)
        {
            try
            {
                var customers = await _orderService.LoadCustomerOrderAsync(true, token: token).ConfigureAwait(false);
                return Ok(customers);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpPost]
        [Route("/data/seed-orders")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var entities = new Dictionary<int, HazelcastJsonValue>();
                for (var i = 1; i <= Constants.fiveMillion; i++)
                {
                    var obj = new Order
                    {
                        Id = i,
                        CustomerId = i,
                        Quantity = i,
                        Price = i,
                        CreatedAt = DateTimeOffset.UtcNow
                    };
                    //entities[i] = obj;
                    string jsonObject = JsonSerializer.Serialize(obj);
                    entities[i] = new HazelcastJsonValue(jsonObject);
                }
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var count = await _orderService.CreateOrderMapAsync(entities, isSetAll: true, token: token).ConfigureAwait(false);
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

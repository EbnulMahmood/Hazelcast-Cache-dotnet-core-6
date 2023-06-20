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
    public class OrderDetailsController : ControllerBase
    {
        private readonly IOrderDetailsService _orderDetailsService;

        public OrderDetailsController(IOrderDetailsService orderDetailsService)
        {
            _orderDetailsService = orderDetailsService;
        }

        [HttpGet]
        [Route("/order-details/with-customer-orders")]
        public async Task<IActionResult> LoadCustomerOrder(CancellationToken token = default)
        {
            try
            {
                var customers = await _orderDetailsService.LoadOrderDetailsWithCustomerOrderAsync(token: token).ConfigureAwait(false);
                return Ok(customers);
            }
            catch (Exception)
            {

                throw;
            }
        }

        [HttpPost]
        [Route("/data/seed-orders-details")]
        public async Task<IActionResult> SeedData(CancellationToken token = default)
        {
            try
            {
                var entities = new Dictionary<int, HazelcastJsonValue>();
                for (var i = 1; i <= Constants.fiveMillion; i++)
                {
                    var obj = new OrderDetails
                    {
                        Id = i,
                        OrderId = i,
                        ProductId = i,
                        IsActive = true
                    };
                    string jsonObject = JsonSerializer.Serialize(obj);
                    entities[i] = new HazelcastJsonValue(jsonObject);
                }
                var watch = System.Diagnostics.Stopwatch.StartNew();
                var count = await _orderDetailsService.CreateOrderDetailsMapAsync(entities, isSetAll: true, token: token).ConfigureAwait(false);
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

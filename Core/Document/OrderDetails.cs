namespace Document
{
    public readonly record struct OrderDetails(int Id, int OrderId, int ProductId, bool IsActive);
    public readonly record struct OrderDetailsWithCustomerOrder(int Id, bool IsActive, int ProductId, int Quantity, double Price, DateTimeOffset OrderDate, string Name, string Address, DateTimeOffset CustomerCretatedAt);
}

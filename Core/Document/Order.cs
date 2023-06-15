namespace Document
{
    public readonly record struct Order(int Id, int CustomerId, int Quantity, double Price, DateTimeOffset CreatedAt);
    public readonly record struct CustomerOrder(int Id, int Quantity, double Price, DateTimeOffset OrderDate, string Name, string Address, DateTimeOffset CustomerCretatedAt);
}

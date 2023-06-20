namespace Document
{
    public readonly record struct OrderDetails(int Id, int OrderId, int ProductId, bool IsActive);
}
